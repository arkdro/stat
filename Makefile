REBAR=$(PWD)/rebar

all:
	$(REBAR) compile

clean:
	$(REBAR) clean
