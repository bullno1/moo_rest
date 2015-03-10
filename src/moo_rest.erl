-module(moo_rest).
-behaviour(cowboy_sub_protocol).
-export([upgrade/4, render_error/4]).

-record(state, {
	env,
	method,
	handler,
	handler_state,
	handler_opts,
	error,
	media_type
}).

upgrade(Req, Env, Module, Opts) ->
	State = #state{
		env = Env,
		handler = Module,
		handler_state = '$pre_init',
		handler_opts = Opts
	},
	method_known(Req, State).

render_error(Error, {<<"text">>, <<"html">>, _}, Req, State) ->
	{io_lib:format("~p", [Error]), Req, State};
render_error(Error, {<<"application">>, <<"json">>, _}, Req, State) ->
	{io_lib:format("\"~p\"", [Error]), Req, State};
render_error(_, undefined, Req, State) ->
	{<<>>, Req, State}.

method_known(Req, State) ->
	{Method, Req2} = cowboy_req:method(Req),
	case method_alias(Method) of
		{ok, Alias} ->
			method_allowed(Req2, State#state{method=Alias});
		error ->
			respond(501, Req2, State)
	end.

method_allowed(Req, #state{method=Method, handler=Module} = State) ->
	case erlang:function_exported(Module, Method, 2) of
		true ->
			negotiate_content_type(Req, State);
		false ->
			respond(405, Req, State#state{error=method_not_allowed})
	end.

negotiate_content_type(Req, State) ->
	case cowboy_req:parse_header(<<"accept">>, Req) of
		{error, badarg} ->
			respond(400, Req, State#state{error=bad_request});
		{ok, undefined, Req2} ->
			Accept = [{{<<"*">>, <<"*">>, []}, 100, []}],
			choose_media_type(Accept, Req2, State);
		{ok, Accept, Req2} ->
			Accept2 = lists:sort(fun compare_accept_types/2, Accept),
			choose_media_type(Accept2, Req2, State)
	end.

compare_accept_types({MediaTypeA, Quality, _}, {MediaTypeB, Quality, _}) ->
	compare_media_types(MediaTypeA, MediaTypeB);
compare_accept_types({_, QualityA, _}, {_, QualityB, _}) ->
	QualityA > QualityB.

compare_media_types({Type, Subtype, ParamsA}, {Type, Subtype, ParamsB}) ->
	length(ParamsA) > length(ParamsB);
compare_media_types({Type, _, _}, {Type, <<"*">>, _}) ->
	true;
compare_media_types({_, _, _}, {<<"*">>, _, _}) ->
	true;
compare_media_types(_, _) ->
	false.

choose_media_type(AcceptTypes, Req, #state{handler=Handler} = State) ->
	DefaultCTP = [
		{<<"text">>, <<"html">>, '*'},
		{<<"application">>, <<"json">>, '*'}
	],
	try call(Handler, content_types_provided, [], DefaultCTP) of
		CTP ->
			case choose_media_type(CTP, AcceptTypes) of
				none ->
					respond(406, Req, State#state{error=not_acceptable});
				MediaType ->
					DefaultVariances =
						case CTP of
							[_, _ | _] -> [<<"accept">>];
							_ -> []
						end,
					Req2 = cowboy_req:set_meta(media_type, MediaType, Req),
					Req3 = cowboy_req:set_resp_header(<<"content-type">>, build_content_type(MediaType), Req2),
					variances(DefaultVariances, Req3, State#state{media_type = MediaType})
			end
	catch
		Class:Error ->
			error_terminate({Class, Error}, content_types_provided, Req, State)
	end.

choose_media_type(_, []) -> none;
choose_media_type(CTP, [{AcceptType, _, _} | Rest]) ->
	case match_media_type(CTP, AcceptType) of
		no_match -> choose_media_type(CTP, Rest);
		MatchedType -> MatchedType
	end.

match_media_type([], _) -> no_match;
match_media_type([TypeA | Rest], TypeB) ->
	case resolve_media_type(TypeA, TypeB) of
		no_solution -> match_media_type(Rest, TypeB);
		Type -> Type
	end.

resolve_media_type('*', Type) -> Type;
resolve_media_type({Type, Subtype, '*'}, {Type, Subtype, _} = TypeB) -> 
	TypeB;
resolve_media_type({Type, Subtype, ParamsA}, {<<"*">>, <<"*">>, ParamsB}) ->
	choose_if_params_match({Type, Subtype, ParamsB}, ParamsA, ParamsB);
resolve_media_type({Type, Subtype, ParamsA}, {Type, <<"*">>, ParamsB}) ->
	choose_if_params_match({Type, Subtype, ParamsB}, ParamsA, ParamsB);
resolve_media_type(_, _) -> no_solution.

choose_if_params_match(Solution, '*', _) -> Solution;
choose_if_params_match(Solution, ParamsA, ParamsB) ->
	case lists:sort(ParamsA) =:= lists:sort(ParamsB) of
		true -> Solution;
		false -> no_solution
	end.

build_content_type({Type, Subtype, Params}) ->
	[Type, $/, Subtype | [[<<"; ">>, Attr, $=, Value] || {Attr, Value} <- Params]].

variances(DefaultVariances, Req, #state{handler=Handler} = State) ->
	try call(Handler, variances, [], DefaultVariances) of
		[] ->
			init(Req, State);
		[Variance] ->
			Req2 = cowboy_req:set_resp_header(<<"vary">>, Variance, Req),
			init(Req2, State);
		[Variance | Rest] ->
			Req2 = cowboy_req:set_resp_header(<<"vary">>, [Variance | [[$,, Var] || Var <- Rest]], Req),
			init(Req2, State)
	catch
		Class:Error ->
			error_terminate({Class, Error}, variances, Req, State)
	end.

init(Req, #state{handler=Module, handler_opts=Opts} = State) ->
	try call(Module, rest_init, [Req, Opts], {ok, Req, no_state}) of
		{ok, Req2, HandlerState} ->
			authorize(Req2, State#state{handler_state=HandlerState});
		{error, Req2, HandlerState} ->
			respond(400, Req2, State#state{handler_state=HandlerState, error=bad_request});
		{{error, Reason}, Req2, HandlerState} ->
			respond(400, Req2, State#state{handler_state=HandlerState, error=Reason});
		{halt, Req2, HandlerState} ->
			terminate(Req2, State#state{handler_state=HandlerState})
	catch
		Class:Error ->
			error_terminate({Class, Error}, rest_init, Req, State)
	end.

authorize(Req, #state{method = Method,
					  handler = Handler,
					  handler_state = HandlerState} = State) ->
	try call(Handler, authorize, [Method, Req, HandlerState], {authorized, Req, HandlerState}) of
		{authorized, Req2, NewHandlerState} ->
			process_request(Method, Req2, State#state{handler_state=NewHandlerState});
		{forbidden, Req2, NewHandlerState} ->
			respond(403, Req2, State#state{handler_state=NewHandlerState, error=forbidden});
		{{unauthorized, AuthHeader}, Req2, NewHandlerState} ->
			Req3 = cowboy_req:set_resp_header(<<"www-authenticate">>, AuthHeader, Req2),
			respond(401, Req3, State#state{handler_state=NewHandlerState, error=unauthorized});
		{halt, Req2, NewHandlerState} ->
			terminate(Req2, State#state{handler_state=NewHandlerState})
	catch
		Class:Error ->
			error_terminate({Class, Error}, authorize, Req, State)
	end.

process_request(Method, Req, #state{handler=Handler, handler_state=HandlerState} = State) when
	  Method =:= get; Method =:= head ->
	try Handler:Method(Req, HandlerState) of
		{ok, Req2, NewHandlerState} ->
			render(render_resource, Req2, State#state{handler_state=NewHandlerState});
		{not_found, Req2, NewHandlerState} ->
			respond(404, Req2, State#state{handler_state=NewHandlerState, error=not_found});
		{gone, Req2, NewHandlerState} ->
			respond(410, Req2, State#state{handler_state=NewHandlerState, error=gone});
		{{moved_permanently, NewUrl}, Req2, NewHandlerState} ->
			Req3 = cowboy_req:set_resp_header(<<"location">>, NewUrl, Req2),
			respond(301, Req3, State#state{handler_state=NewHandlerState});
		{{moved_temporarily, NewUrl}, Req2, NewHandlerState} ->
			Req3 = cowboy_req:set_resp_header(<<"location">>, NewUrl, Req2),
			respond(307, Req3, State#state{handler_state=NewHandlerState});
		{error, Req2, NewHandlerState} ->
			respond(400, Req2, State#state{handler_state=NewHandlerState, error=bad_request});
		{{error, Reason}, Req2, NewHandlerState} ->
			respond(400, Req2, State#state{handler_state=NewHandlerState, error=Reason});
		{halt, Req2, NewHandlerState} ->
			terminate(Req2, State#state{handler_state=NewHandlerState})
	catch
		Class:Error ->
			error_terminate({Class, Error}, Method, Req, State)
	end;

process_request(Method, Req, State) when
	  Method =:= put; Method =:= post; Method =:= patch ->
	case cowboy_req:parse_header(<<"content-type">>, Req) of
		{ok, ContentType, Req2} ->
			parse_body(ContentType, Method, Req2, State);
		{error, badarg} ->
			respond(415, Req, State#state{error=unsupported})
	end;

process_request(delete, Req, #state{handler=Handler, handler_state=HandlerState} = State) ->
	try Handler:delete(Req, HandlerState) of
		{ok, Req2, NewHandlerState} ->
			render(render_result, Req2, State#state{handler_state=NewHandlerState});
		{not_found, Req2, NewHandlerState} ->
			respond(404, Req2, State#state{handler_state=NewHandlerState, error=not_found});
		{gone, Req2, NewHandlerState} ->
			respond(410, Req2, State#state{handler_state=NewHandlerState, error=gone});
		{{moved_permanently, NewUrl}, Req2, NewHandlerState} ->
			Req3 = cowboy_req:set_resp_header(<<"location">>, NewUrl, Req2),
			respond(301, Req3, State#state{handler_state=NewHandlerState});
		{{moved_temporarily, NewUrl}, Req2, NewHandlerState} ->
			Req3 = cowboy_req:set_resp_header(<<"location">>, NewUrl, Req2),
			respond(307, Req3, State#state{handler_state=NewHandlerState});
		{halt, Req2, NewHandlerState} ->
			terminate(Req2, State#state{handler_state=NewHandlerState})
	catch
		Class:Error ->
			error_terminate({Class, Error}, delete, Req, State)
	end.

parse_body(ContentType, Method, Req,  #state{handler=Handler, handler_state=HandlerState} = State) ->
	try call(Handler, parse_body, [ContentType, Req, HandlerState], {ok, Req, HandlerState}) of
		{ok, Req2, NewHandlerState} ->
			accept_resource(Method, Req2, State#state{handler_state=NewHandlerState});
		{error, Req2, NewHandlerState} ->
			respond(400, Req2, State#state{handler_state=NewHandlerState, error=bad_request});
		{{error, Reason}, Req2, NewHandlerState} ->
			respond(400, Req2, State#state{handler_state=NewHandlerState, error=Reason});
		{unsupported, Req2, NewHandlerState} ->
			respond(415, Req2, State#state{handler_state=NewHandlerState, error=unsupported});
		{halt, Req2, NewHandlerState} ->
			terminate(Req2, State#state{handler_state=NewHandlerState})
	catch
		Class:Error ->
			error_terminate({Class, Error}, parse_body, Req, State)
	end.

accept_resource(Method, Req, #state{handler=Handler, handler_state=HandlerState} = State) ->
	try Handler:Method(Req, HandlerState) of
		{ok, Req2, NewHandlerState} ->
			render(render_result, Req2, State#state{handler_state=NewHandlerState});
		{{see, Url}, Req2, NewHandlerState} when Method =:= post ->
			Req3 = cowboy_req:set_resp_header(<<"location">>, Url, Req2),
			respond(303, Req3, State#state{handler_state=NewHandlerState});
		{{created, Url}, Req2, NewHandlerState} when Method =/= patch ->
			Req3 = cowboy_req:set_resp_header(<<"location">>, Url, Req2),
			respond(201, Req3, State#state{handler_state=NewHandlerState});
		{conflict, Req2, NewHandlerState} when Method =:= put; Method =:= post ->
			respond(409, Req2, State#state{handler_state=NewHandlerState, error=conflict});
		{unsupported, Req2, NewHandlerState} ->
			respond(415, Req2, State#state{handler_state=NewHandlerState, error=unsupported});
		{not_found, Req2, NewHandlerState} when Method =/= put ->
			respond(404, Req2, State#state{handler_state=NewHandlerState, error=not_found});
		{{moved_permanently, NewUrl}, Req2, NewHandlerState} ->
			Req3 = cowboy_req:set_resp_header(<<"location">>, NewUrl, Req2),
			respond(301, Req3, State#state{handler_state=NewHandlerState});
		{{moved_temporarily, NewUrl}, Req2, NewHandlerState} ->
			Req3 = cowboy_req:set_resp_header(<<"location">>, NewUrl, Req2),
			respond(307, Req3, State#state{handler_state=NewHandlerState});
		{error, Req2, NewHandlerState} ->
			respond(400, Req2, State#state{handler_state=NewHandlerState, error=bad_request});
		{{error, Reason}, Req2, NewHandlerState} ->
			respond(400, Req2, State#state{handler_state=NewHandlerState, error=Reason});
		{halt, Req2, NewHandlerState} ->
			terminate(Req2, State#state{handler_state=NewHandlerState})
	catch
		Class:Error ->
			error_terminate({Class, Error}, Method, Req, State)
	end.

render(Method, Req, #state{env=Env, handler=Handler, handler_state=HandlerState, media_type=MediaType} = State) ->
	try call(Handler, Method, [MediaType, Req, HandlerState], {<<>>, Req, HandlerState}) of
		{<<>>, Req2, _} ->
			{ok, Req3} = cowboy_req:reply(204, Req2),
			{ok, Req3, Env};
		{Content, Req2, _} ->
			{ok, Req3} = cowboy_req:reply(200, [], Content, Req2),
			{ok, Req3, Env}
	catch
		Class:Error ->
			error_terminate({Class, Error}, Method, Req, State)
	end.

method_alias(<<"GET">>)    -> {ok, get};
method_alias(<<"HEAD">>)   -> {ok, head};
method_alias(<<"POST">>)   -> {ok, post};
method_alias(<<"PUT">>)    -> {ok, put};
method_alias(<<"PATCH">>)  -> {ok, patch};
method_alias(<<"DELETE">>) -> {ok, delete};
method_alias(_)            -> error.

call(M, F, A, Default) ->
	case erlang:function_exported(M, F, length(A)) of
		true -> apply(M, F, A);
		false ->
			case is_function(Default) of
				true -> apply(Default, A);
				false -> Default
			end
	end.

respond(Status, Req, #state{env=Env,
                            handler=Handler,
                            handler_state=HandlerState,
                            media_type=MediaType,
                            error=Error} = State) when
	  400 =< Status, Status < 500 ->
	try call(Handler, render_error, [Error, MediaType, Req, HandlerState], fun moo_rest:render_error/4) of
		{Content, Req2, _} ->
			{ok, Req3} = cowboy_req:reply(Status, [], Content, Req2),
			{ok, Req3, Env}
	catch
		Class:Error ->
			error_terminate({Class, Error}, render_error, Req, State)
	end;

respond(Status, Req, State) ->
	{ok, Req2} = cowboy_req:reply(Status, Req),
	terminate(Req2, State).

error_terminate(Reason, Method, Req, #state{handler=Module, handler_state=HandlerState} = State) ->
	_ = error_logger:error_report([
		moo_rest_crashed,
		{reason, Reason},
		{module, Module},
		{method, Method},
		{state, HandlerState},
		{stacktrace, erlang:get_stacktrace()}
	]),
	{ok, Req2} = cowboy_req:reply(500, Req),
	terminate(Req2, State).

terminate(Req, #state{env=Env, handler_state='$pre_init'}) ->
	{ok, Req, Env};
terminate(Req, #state{env=Env, handler=Handler, handler_state=HandlerState}) ->
	try call(Handler, rest_terminate, [cowboy_req:lock(Req), HandlerState], ok) of
		_ -> ok
	catch
		Class:Error ->
			_ = error_logger:error_report([
				moo_rest_crashed,
				{reason, {Class, Error}},
				{module, Handler},
				{method, rest_terminate},
				{state, HandlerState},
				{stacktrace, erlang:get_stacktrace()}
			])
	end,
	{ok, Req, Env}.
