implement Ventiread;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "arg.m";
include "dial.m";
	dial: Dial;
include "env.m";
	env: Env;
include "venti.m";
	venti: Venti;
	Score, Session: import venti;

Ventiread: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

addr := "$venti";
dflag := 0;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	env = load Env Env->PATH;
	venti = load Venti Venti->PATH;
	venti->init();

	ev := env->getenv("venti");
	if(ev != nil)
		addr = ev;

	arg->init(args);
	arg->setusage(arg->progname() + " [-d] [-a addr] type score");
	while((c := arg->opt()) != 0)
		case c {
		'a' =>	addr = arg->earg();
		'd' =>	dflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 2)
		arg->usage();

	stype := int hd args;
	(sok, score) := Score.parse(hd tl args);
	if(sok != 0)
		error("bad score: "+ hd tl args);

	say("dialing");
	addr = dial->netmkaddr(addr, "net", "venti");
	cc := dial->dial(addr, nil);
	if(cc == nil)
		error(sprint("dialing %s: %r", addr));
	fd := cc.dfd;
	say("have connection");

	session := Session.new(fd);
	if(session == nil)
		error(sprint("handshake: %r"));
	say("have handshake");

	d := session.read(score, stype, Venti->Maxlumpsize);
	if(d == nil)
		error(sprint("reading score: %r"));
	say(sprint("have data, length %d", len d));
	sys->write(sys->fildes(1), d, len d);
}

error(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
	raise "fail:"+s;
}

say(s: string)
{
	if(dflag)
		sys->fprint(sys->fildes(2), "%s\n", s);
}
