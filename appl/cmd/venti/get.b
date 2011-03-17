implement Ventiget;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "bufio.m";
	bufio: Bufio;
	Iobuf: import bufio;
include "arg.m";
include "dial.m";
	dial: Dial;
include "string.m";
	str: String;
include "venti.m";
	venti: Venti;
	Score, Session, Dirtype, Datatype: import venti;
include "vac.m";
	vac: Vac;
	Vacfile, Entry, Entrysize: import vac;

Ventiget: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

addr := "$venti";
dflag := 0;
session: ref Session;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	str = load String String->PATH;
	venti = load Venti Venti->PATH;
	venti->init();
	vac = load Vac Vac->PATH;
	vac->init();

	arg->init(args);
	arg->setusage(sprint("%s [-d] [-a addr] [entry:]score", arg->progname()));
	while((c := arg->opt()) != 0)
		case c {
		'a' =>	addr = arg->earg();
		'd' =>	dflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 1)
		arg->usage();

	(tag, scorestr) := str->splitstrr(hd args, ":");
	if(tag != nil)
		tag = tag[:len tag-1];
	if(tag == nil)
		tag = "entry";
	if(tag != "entry")
		fail("bad score type: "+tag);

	(sok, score) := Score.parse(scorestr);
	if(sok != 0)
		fail("bad score: "+scorestr);
	say("have score");

	addr = dial->netmkaddr(addr, "net", "venti");
	cc := dial->dial(addr, nil);
	if(cc == nil)
		fail(sprint("dialing %s: %r", addr));
	say("have connection");

	fd := cc.dfd;
	session = Session.new(fd);
	if(session == nil)
		fail(sprint("handshake: %r"));
	say("have handshake");

	d := session.read(score, Dirtype, Entrysize);
	if(d == nil)
		fail(sprint("reading entry: %r"));
	e := Entry.unpack(d);
	if(e == nil)
		fail(sprint("unpacking entry: %r"));
	say("have entry");

	bio := bufio->fopen(sys->fildes(1), bufio->OWRITE);
	if(bio == nil)
		fail(sprint("bufio fopen: %r"));

	say("reading");
	buf := array[sys->ATOMICIO] of byte;
	vf := Vacfile.new(session, e);
	for(;;) {
		rn := vf.read(buf, len buf);
		if(rn == 0)
			break;
		if(rn < 0)
			fail(sprint("reading: %r"));
		wn := bio.write(buf, rn);
		if(wn != rn)
			fail(sprint("writing: %r"));
	}
	bok := bio.flush();
	bio.close();
	if(bok == bufio->ERROR || bok == bufio->EOF)
		fail(sprint("bufio close: %r"));
	say("done");
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
