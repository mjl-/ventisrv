implement Venticopy;

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
	Pointertype0, Pointertype6, Score, Session, Roottype, Dirtype, Datatype, Scoresize: import venti;
include "vac.m";
	vac: Vac;
	Root, Entry, Rootsize, Entrysize: import vac;

Venticopy: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

dflag, fflag: int;
session: ref Session;
srcs, dsts: ref Session;

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
	arg->setusage(sprint("%s [-df] srcaddr dstaddr [vac:]score", arg->progname()));
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'f' =>	fflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 3)
		arg->usage();

	srcaddr := hd args;
	dstaddr := hd tl args;
	args = tl tl args;

	(tag, scorestr) := str->splitstrr(hd args, ":");
	if(tag != nil)
		tag = tag[:len tag-1];
	t: int;
	case tag {
	"vac" =>	t = Roottype;
	"entry" =>	t = Dirtype;
	* =>	sys->fprint(sys->fildes(2), "bad score type\n");
		arg->usage();
	}

	(sok, score) := Score.parse(scorestr);
	if(sok != 0)
		fail("bad score: "+scorestr);
	say("have score");

	srcs = vdial(srcaddr);
	dsts = vdial(dstaddr);

	walk(score, t, 0);
	say("have walk");

	if(dsts.sync() < 0)
		fail(sprint("syncing destination: %r"));
	say("synced");
}

walk(s: Score, t, dt: int)
{
	say(sprint("walk: %s/%d", s.text(), t));

	if(fflag && dsts.read(s, t, Venti->Maxlumpsize) != nil) {
		say(sprint("skipping %s/%d", s.text(), t));
		return;
	}

	d := srcs.read(s, t, Venti->Maxlumpsize);
	if(d == nil)
		fail(sprint("reading %s/%d: %r", s.text(), t));

	case t {
	Roottype =>
		r := Root.unpack(d);
		if(r == nil)
			fail(sprint("bad root: %r"));
		walk(r.score, Dirtype, 0);
		if(!isnul(*r.prev))
			walk(*r.prev, Roottype, 0);

	Dirtype =>
		for(o := 0; o+Entrysize <= len d; o += Entrysize) {
			e := Entry.unpack(d[o:o+Entrysize]);
			if(e == nil)
				fail(sprint("bad entry: %r"));
			if(!(e.flags&Venti->Entryactive))
				continue;
			nt := Datatype;
			if(e.flags&Venti->Entrydir)
				nt = Dirtype;
			if(e.depth == 0)
				walk(e.score, nt, 0);
			else
				walk(e.score, Pointertype0-1+e.depth, nt);
		}
		
	Pointertype0 to Pointertype6 =>
		nt := t-1;
		for(o := 0; o+Scoresize <= len d; o += Scoresize) {
			ns := Score(d[o:o+Scoresize]);
			if(ns.eq(Score.zero()))
				continue;
			if(t == Pointertype0)
				nt = dt;
			walk(ns, nt, dt);
		}

	Datatype =>
		;

	* =>
		fail(sprint("unknown block type, %s/%d", s.text(), t));
	}
	
	(ok, ns) := dsts.write(t, d);
	if(ok < 0)
		fail(sprint("writing %s/%d: %r", ns.text(), t));
	if(!ns.eq(s))
		fail(sprint("destination returned different score: %s versus %s", s.text(), ns.text()));
}

isnul(s: Score): int
{
	for(i := 0; i < len s.a; i++)
		if(s.a[i] != byte 0)
			return 0;
	return 1;
}

vdial(addr: string): ref Session
{
	addr = dial->netmkaddr(addr, "net", "venti");
	cc := dial->dial(addr, nil);
	if(cc == nil)
		fail(sprint("dialing %s: %r", addr));
	say("have connection");

	session = Session.new(cc.dfd);
	if(session == nil)
		fail(sprint("handshake: %r"));
	say("have handshake");
	return session;
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
