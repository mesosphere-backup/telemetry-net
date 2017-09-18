.PHONY: test

all: compile

##
## Compilation targets
##

compile:
	./rebar3 compile

clean:
	./rebar3 clean

##
## Test targets
##

check: test xref dialyzer

test: ct eunit

eunit:
	./rebar3 eunit -v

ct:
	./rebar3 ct -v

dialyzer:
	./rebar3 dialyzer

xref:
	./rebar3 xref
