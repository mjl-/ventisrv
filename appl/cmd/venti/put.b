implement Ventiput;

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
include "venti.m";
	venti: Venti;
	Score, Session, Dirtype, Datatype: import venti;
include "vac.m";
	vac: Vac;
	File, Entry: import vac;

Ventiput: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

addr := "$venti";
dflag := 0;
blocksize := Vac->Dsize;
session: ref Session;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	bufio = load Bufio Bufio->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	venti = load Venti Venti->PATH;
	venti->init();
	vac = load Vac Vac->PATH;
	vac->init();

	arg->init(args);
	arg->setusage(sprint("%s [-d] [-a addr] [-b blocksize]", arg->progname()));
	while((c := arg->opt()) != 0)
		case c {
		'a' =>	addr = arg->earg();
		'b' =>	blocksize = int arg->earg();
		'd' =>	dflag++;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 0)
		arg->usage();

	addr = dial->netmkaddr(addr, "net", "venti");
	cc := dial->dial(addr, nil);
	if(cc == nil)
		error(sprint("dialing %s: %r", addr));
	say("have connection");

	session = Session.new(cc.dfd);
	if(session == nil)
		error(sprint("handshake: %r"));
	say("have handshake");

	bio := bufio->fopen(sys->fildes(0), bufio->OREAD);
	if(bio == nil)
		error(sprint("bufio open: %r"));

	say("writing");
	f := File.new(session, Datatype, blocksize);
	for(;;) {
		buf := array[blocksize] of byte;
		n := 0;
		while(n < len buf) {
			want := len buf - n;
			have := bio.read(buf[n:], want);
			if(have == 0)
				break;
			if(have < 0)
				error(sprint("reading: %r"));
			n += have;
		}
		if(dflag) say(sprint("have buf, length %d", n));

		if(f.write(buf[:n]) < 0)
			error(sprint("writing: %r"));
		if(n != len buf)
			break;
	}
	bio.close();
	e := f.finish();
	if(e == nil)
		error(sprint("flushing: %r"));
	d := e.pack();

	(rok, rscore) := session.write(Dirtype, d);
	if(rok < 0)
		error(sprint("writing root score: %r"));
	say("entry written, "+rscore.text());
	sys->print("entry:%s\n", rscore.text());

	if(session.sync() < 0)
		error(sprint("syncing server: %r"));
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
