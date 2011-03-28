implement Venticopy;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
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

dflag: int;
fflag: int;
srcs: ref Session;
dsts: ref Session;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	str = load String String->PATH;
	venti = load Venti Venti->PATH;
	venti->init();
	vac = load Vac Vac->PATH;
	vac->init();

	arg->init(args);
	arg->setusage(arg->progname()+" [-df] srcaddr dstaddr [vac:]score");
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
	ts := hd tl tl args;

	(tag, ss) := str->splitstrr(ts, ":");
	t: int;
	case tag {
	"vac:" =>	t = venti->Roottype;
	"entry:" =>	t = venti->Dirtype;
	* =>		fail("bad score type");
	}
	(ok, score) := Score.parse(ss);
	if(ok != 0)
		fail("bad score: "+ss);

	srcs = vdial(srcaddr);
	dsts = vdial(dstaddr);
	walk(score, t, 0);
	if(dsts.sync() < 0)
		fail(sprint("syncing destination: %r"));
}

vdial(addr: string): ref Session
{
	addr = dial->netmkaddr(addr, "net", "venti");
	c := dial->dial(addr, nil);
	if(c == nil)
		fail(sprint("dialing %s: %r", addr));

	s := Session.new(c.dfd);
	if(s == nil)
		fail(sprint("handshake: %r"));
	return s;
}

walk(s: Score, t, dt: int)
{
	say(sprint("walk: %s/%d", s.text(), t));

	if(fflag && dsts.read(s, t, venti->Maxlumpsize) != nil)
		return say(sprint("skipping %s/%d", s.text(), t));

	d := srcs.read(s, t, venti->Maxlumpsize);
	if(d == nil)
		fail(sprint("reading %s/%d: %r", s.text(), t));

	case t {
	venti->Roottype =>
		r := Root.unpack(d);
		if(r == nil)
			fail(sprint("bad root: %r"));
		walk(r.score, venti->Dirtype, 0);
		if(!isnul(*r.prev))
			walk(*r.prev, venti->Roottype, 0);

	venti->Dirtype =>
		for(o := 0; o+Entrysize <= len d; o += Entrysize) {
			e := Entry.unpack(d[o:o+Entrysize]);
			if(e == nil)
				fail(sprint("bad entry: %r"));
			if(!(e.flags&venti->Entryactive))
				continue;
			nt := venti->Datatype;
			if(e.flags&venti->Entrydir)
				nt = venti->Dirtype;
			if(e.depth == 0)
				walk(e.score, nt, 0);
			else
				walk(e.score, venti->Pointertype0-1+e.depth, nt);
		}
		
	venti->Pointertype0 to venti->Pointertype6 =>
		nt := t-1;
		for(o := 0; o+Scoresize <= len d; o += Scoresize) {
			ns := Score(d[o:o+Scoresize]);
			if(ns.eq(Score.zero()))
				continue;
			if(t == venti->Pointertype0)
				nt = dt;
			walk(ns, nt, dt);
		}

	venti->Datatype =>
		{}

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

say(s: string)
{
	if(dflag)
		warn(s);
}

warn(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
}

fail(s: string)
{
	warn(s);
	raise "fail:"+s;
}
