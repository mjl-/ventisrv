implement Ventisync;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "arg.m";
include "venti.m";
	venti: Venti;
	Score, Session: import venti;

Ventisync: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

addr := "net!$venti!venti";
dflag := 0;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	venti = load Venti Venti->PATH;
	venti->init();

	arg->init(args);
	arg->setusage(arg->progname() + " [-d] [-a addr]");
	while((c := arg->opt()) != 0)
		case c {
		'a' =>	addr = arg->earg();
		'd' =>	dflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 0)
		arg->usage();

	say("dialing");
	(cok, conn) := sys->dial(addr, nil);
	if(cok < 0)
		fail(sprint("dialing %s: %r", addr));
	fd := conn.dfd;
	say("have connection");

	session := Session.new(fd);
	if(session == nil)
		fail(sprint("handshake: %r"));
	say("have handshake");

	if(session.sync() < 0)
		fail(sprint("sync: %r"));
	say("synced");
}

fail(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
	raise "fail:"+s;
}

say(s: string)
{
	if(dflag)
		sys->fprint(sys->fildes(2), "%s\n", s);
}
