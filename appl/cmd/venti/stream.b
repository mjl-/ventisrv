# like venti/copy, but queue up to 256 requests to source and destination.
# this makes operation over high latency networks bearable.

implement VentiStream;

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
	Scoresize, Entrysize, Vmsg, Root, Entry, Session, Score: import venti;

VentiStream: module {
	init:	fn(nil: ref Draw->Context, nil: list of string);
};

dflag: int;
Oflag: int;
tflag: int;
vflag: int;

Stat: adt {
	types:	array of int;	# counts of message types
	n:	int;		# total requests
	ms:	int;		# total time requests were underway
	minms:	int;
	maxms:	int;		
	maxq:	int;		# max requests queued
};
sstat: Stat;
dstat: Stat;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	str = load String String->PATH;
	venti = load Venti Venti->PATH;
	venti->init();

	sys->pctl(sys->NEWPGRP, nil);

	arg->init(args);
	arg->setusage(arg->progname()+" [-dOtv] srcaddr dstaddr [tag:]score");
	while((c := arg->opt()) != 0)
		case c {
		'd' =>	dflag++;
		'O' =>	Oflag++;
		't' =>	tflag++;
		'v' =>	vflag++;
		* =>
			arg->usage();
		}
	args = arg->argv();
	if(len args != 3)
		arg->usage();

	srcaddr := dial->netmkaddr(hd args, "net", "venti");
	dstaddr := dial->netmkaddr(hd tl args, "net", "venti");
	(tag, ss) := str->splitstrr(hd tl tl args, ":");

	(ok, s) := Score.parse(ss);
	if(ok < 0)
		fail("bad score");
	t: int;
	case tag {
	"vac:" or
	"" =>	t = venti->Roottype;
	"entry:" =>
		t = venti->Dirtype;
	* =>	fail(sprint("unknown tag %#q", tag));
	}

	sfd := vdial(srcaddr);
	dfd := vdial(dstaddr);

	sstat.types = array[venti->Maxtype] of {* => 0};
	dstat.types = array[venti->Maxtype] of {* => 0};
	sstat.minms = (1<<31)-1;
	dstat.minms = (1<<31)-1;

	run(sfd, dfd, t, s);
	killgrp(pid());
	if(vflag) {
		statprint("src", sstat);
		statprint("dst", dstat);
	}
}

statprint(who: string, s: Stat)
{
	warn(sprint("%s: n %d, avg min max %d %d %d ms, maxq %d", who, s.n, s.ms/s.n, s.minms, s.maxms, s.maxq));
	if(vflag > 1) {
		warn(sprint("\troot\t%d", s.types[venti->Roottype]));
		warn(sprint("\tdata\t%d", s.types[venti->Datatype]));
		warn(sprint("\tdir\t%d", s.types[venti->Dirtype]));
		for(i := venti->Pointertype0; i <= venti->Pointertype6; i++)
			warn(sprint("\tptr%d\t%d", i-venti->Pointertype0, s.types[i]));
	}
}

Work: adt {
	t:	int;
	s:	Score;
	d:	array of byte;
	dt:	int;	# eventual datatype/dirtype, in case t is pointertype
	ms:	int;
};

C: adt {
	b:	array of byte;	# scores
	n:	int;		# used
	next:	ref C;
};

State: adt {
	stids,
	dtids:	list of int;
	stc,
	src,
	dtc,
	drc:	chan of ref Vmsg;
	stid,
	dtid:	array of ref Work;
	swork,
	dwork:	list of ref Work;
	chains:	array of ref C;
};

statenew(): ref State
{
	stc := chan[256] of ref Vmsg;
	src := chan[256] of ref Vmsg;
	dtc := chan[256] of ref Vmsg;
	drc := chan[256] of ref Vmsg;

	stids: list of int;
	dtids: list of int;
	for(i := 0; i < 256; i++) {
		stids = i::stids;
		dtids = i::dtids;
	}

	stid := array[256] of ref Work;
	dtid := array[256] of ref Work;

	chains := array[2**12] of ref C;

	return ref State(stids, dtids, stc, src, dtc, drc, stid, dtid, nil, nil, chains);
}

entryseen(st: ref State, s: Score): int
{
	x := int s.a[0]<<8|int s.a[1]<<0;
	x &= len st.chains-1;
	for(c := st.chains[x]; c != nil; c = c.next)
		for(o := Scoresize*(c.n-1); o >= 0; o -= Scoresize)
			if(s.eq(Score(c.b[o:o+Scoresize])))
				return 1;
	c = st.chains[x];
	if(c == nil || c.n == 8)
		st.chains[x] = c = ref C(array[8*Scoresize] of byte, 0, c);
	c.b[Scoresize*c.n++:] = s.a;
	return 0;
}

run(sfd, dfd: ref Sys->FD, t: int, s: Score)
{
	errc := chan of string;
	st := statenew();

	zeroscore := Score.zero();
	nullscore := Score(array[Scoresize] of {* => byte 0});

	spawn write(sfd, st.stc, errc, "src");
	spawn read(sfd, st.src, errc, "src");
	spawn write(dfd, st.dtc, errc, "dst");
	spawn read(dfd, st.drc, errc, "dst");

	# kick off
	sstat.types[t]++;
	sstat.n++;
	mm := ref Vmsg.Tread(1, hd st.stids, s, t, venti->Maxlumpsize);
	st.stids = tl st.stids;
	st.stid[mm.tid] = ref Work(t, s, nil, -1, sys->millisec());
	st.stc <-= mm;

	sync := 0;

	for(;;)
	alt {
	err := <-errc =>
		fail(err);

	vv := <-st.src =>
		ow := st.stid[vv.tid];
		if(ow == nil)
			fail("unknown tid from src");
		st.stid[vv.tid] = nil;
		st.stids = vv.tid::st.stids;

		now: int;
		if(vflag) {
			now = sys->millisec();
			ms := now-ow.ms;
			sstat.ms += ms;
			sstat.minms = min(sstat.minms, ms);
			sstat.maxms = max(sstat.maxms, ms);
		}

		pick v := vv {
		Rread =>
			case ow.t {
			venti->Roottype =>
				r := venti->unpackroot(v.data);
				if(r == nil)
					fail(sprint("bad root from src: %r"));
				if(!Oflag && !r.prev.eq(nullscore))
					st.swork = ref Work(venti->Roottype, r.prev, nil, -1, -1)::st.swork;
				st.swork = ref Work(venti->Dirtype, r.score, nil, -1, -1)::st.swork;
			venti->Dirtype =>
				d := v.data;
				if(len d % Entrysize != 0) {
					nd := array[(len d/Entrysize+1)*Entrysize] of byte;
					nd[(len d/Entrysize)*Entrysize:] = array[Entrysize] of {* => byte 0};
					nd[:] = d;
					d = nd;
				}
				for(o := 0; o+venti->Entrysize <= len d; o += venti->Entrysize) {
					e := venti->unpackentry(d[o:o+venti->Entrysize]);
					if(e == nil)
						fail(sprint("bad entry from src: %r"));
					if((e.flags&venti->Entryactive) == 0 || entryseen(st, e.score))
						continue;
					dt := venti->Datatype;
					if(e.flags&venti->Entrydir)
						dt = venti->Dirtype;
					if(e.depth == 0)
						nw := ref Work(dt, e.score, nil, -1, -1);
					else
						nw = ref Work(venti->Pointertype0+e.depth-1, e.score, nil, dt, -1);
					st.swork = nw::st.swork;
				}
			venti->Datatype =>
				{}
			venti->Pointertype0 to
			venti->Pointertype6 =>
				if(ow.t == venti->Pointertype0)
					nt := ow.dt;
				else
					nt = ow.t-1;
				d := v.data;
				if(len d % Scoresize != 0)
					warn(sprint("bad pointer block, length %d but should be multiple of scoresize 20", len d));
				for(o := 0; o+Scoresize <= len d; o += Scoresize)
					if(!zeroscore.eq(ns := Score(d[o:o+Scoresize])))
						st.swork = ref Work(nt, ns, nil, ow.dt, -1)::st.swork;
			* =>
				fail(sprint("bad block type %d from src", ow.t));
			}
			ow.ms = -1;
			ow.d = v.data;
			st.dwork = ow::st.dwork;
			sync = sched(st, now);
		Rerror =>
			fail("error from src: "+v.e);
		* =>
			fail("unexpected vmsg from src");
		}

	vv := <-st.drc =>
		ow := st.dtid[vv.tid];
		if(ow == nil)
			fail("unknown tid from dst");
		st.dtid[vv.tid] = nil;
		st.dtids = vv.tid::st.dtids;

		now := sys->millisec();
		ms := now-ow.ms;
		dstat.ms += ms;
		dstat.minms = min(dstat.minms, ms);
		dstat.maxms = max(dstat.maxms, ms);

		pick v := vv {
		Rwrite =>
			if(!v.score.eq(ow.s))
				fail(sprint("dst returned %s, expected %s", v.score.text(), ow.s.text()));
			sync = sched(st, now);
		Rerror =>
			fail("error from dst: "+v.e);
		Rsync =>
			if(!sync)
				fail("bogus sync response from dst");
			return say("done and synced");
		* =>
			fail("unexpected vmsg from dst");
		}
	}
}

sched(st: ref State, now: int): int
{
	while(st.dwork != nil && st.dtids != nil) {
		w := hd st.dwork;
		tid := hd st.dtids;

		m := ref Vmsg.Twrite(1, tid, w.t, w.d);
		w.d = nil;
		w.ms = now;
		st.dtid[tid] = w;
		st.dtc <-= m;
		if(dflag) warn(sprint("stream: %s/%d", w.s.text(), w.t));
		dstat.types[w.t]++;
		dstat.n++;

		st.dwork = tl st.dwork;
		st.dtids = tl st.dtids;
	}
	for(more := 256-len st.dwork; more > 0 && st.swork != nil && st.stids != nil; more--) {
		w := hd st.swork;
		tid := hd st.stids;

		m := ref Vmsg.Tread(1, tid, w.s, w.t, venti->Maxlumpsize);
		w.ms = now;
		st.stid[tid] = w;
		st.stc <-= m;
		sstat.types[w.t]++;
		sstat.n++;

		st.swork = tl st.swork;
		st.stids = tl st.stids;
	}
	sstat.maxq = max(sstat.maxq, 256-len st.stids);
	dstat.maxq = max(dstat.maxq, 256-len st.dtids);
	done := len st.dtids == 256 && len st.stids == 256 && st.swork == nil && st.dwork == nil;
	if(done) {
		tid := hd st.dtids;
		nw := ref Work;
		nw.ms = now;
		st.dtid[tid] = nw;
		st.dtc <-= ref Vmsg.Tsync(1, tid);
	}
	return done;
}

read(fd: ref Sys->FD, c: chan of ref Vmsg, errc: chan of string, who: string)
{
	for(;;) {
		(v, err) := Vmsg.read(fd);
		if(err != nil) {
			errc <-= err;
			return;
		}
		if(tflag) warn(who+" <- "+v.text());
		c <-= v;
	}
}

write(fd: ref Sys->FD, c: chan of ref Vmsg, errc: chan of string, who: string)
{
	for(;;) {
		v := <-c;
		if(tflag) warn(who+" -> "+v.text());
		if(sys->write(fd, d := v.pack(), len d) != len d)
			errc <-= sprint("write: %r");
	}
}

vdial(addr: string): ref Sys->FD
{
	c := dial->dial(addr, nil);
	if(c == nil)
		fail(sprint("dial %q: %r", addr));
	s := Session.new(c.dfd);
	if(s == nil)
		fail(sprint("handshake %q: %r", addr));
	return c.dfd;
}

min(a, b: int): int
{
	if(a < b)
		return a;
	return b;
}

max(a, b: int): int
{
	if(a > b)
		return a;
	return b;
}

pid(): int
{
	return sys->pctl(0, nil);
}

progctl(pid: int, s: string)
{
	sys->fprint(sys->open(sprint("/prog/%d/ctl", pid), Sys->OWRITE), "%s", s);
}

killgrp(pid: int)
{
	progctl(pid, "killgrp");
}

say(s: string)
{
	if(dflag)
		warn(s);
}

fd2: ref Sys->FD;
warn(s: string)
{
	if(fd2 == nil)
		fd2 = sys->fildes(2);
	sys->fprint(fd2, "%s\n", s);
}

fail(s: string)
{
	warn(s);
	raise "fail:"+s;
}
