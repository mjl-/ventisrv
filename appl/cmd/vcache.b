# possible improvements
# - perhaps not unpack,pack messages all the time, but reuse the byte array, only changing the tid
# - when disconnecting a client, send an Rerror to still open request instead of just closing the tcp connection?

implement Vcache;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "arg.m";
include "dial.m";
	dial: Dial;
include "string.m";
	str: String;
include "keyring.m";
	kr: Keyring;
include "venti.m";
	venti: Venti;
	Score, Session, Vmsg: import venti;

Vcache: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

Eperm:	con "permission denied";

laddr := "*";
statsdir := "/chan/";
statsfile := "vcachestats";
dflag,
nflag,
vflag,
wflag: int;

maxcachesize: int;
remoteaddr: string;
proxyaddr: string;
clients: list of ref Client;
remote: ref Conn;
proxy: ref Conn;

wrotec: chan of (int, int);
registerc: chan of (int, int, chan of ref Vmsg);
requestc: chan of (ref Vmsg, int);

proxymiss: int;
proxyreq: int;
remotereads: int;
remotewrites: int;

Block: adt {
	s:		Score;
	dtype:		int;
	d:		array of byte;
	used:		byte;
	lookprev,
	looknext:	cyclic ref Block;
	takeprev,
	takenext:	cyclic ref Block;
};
Arraysize:	con 4+4+4;
Refsize:	con 4;
Blocksize:	con Arraysize+Venti->Scoresize + 1 + Arraysize + 1 + Arraysize + 4*Refsize;
Dataavgsize:	con 8*1024+Blocksize;	# 8kb blocks are not really average...
Blocksperhead:	con 32;
		
take: ref Block;
cacheused: int;
cachemiss: int;
cachereq: int;
cacheheads: array of ref Block;

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	dial = load Dial Dial->PATH;
	str = load String String->PATH;
	kr = load Keyring Keyring->PATH;
	venti = load Venti Venti->PATH;
	venti->init();

	arg->init(args);
	arg->setusage(arg->progname()+" [-dnvw] [-a laddr] [-s size] [-S statsfile] remoteaddr [proxyaddr]");
	while((c := arg->opt()) != 0)
		case c {
		'a' =>	laddr = arg->earg();
		'd' =>	dflag++;
		'n' =>	nflag++;
		's' =>	maxcachesize = int arg->earg();
		'S' =>	(statsdir, statsfile) = str->splitstrr(arg->earg(), "/");
			if(statsfile == nil) {
				warn("bad statsfile");
				arg->usage();
			}
		'v' =>	vflag++;
		'w' =>	wflag = 1;
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args != 1 && len args != 2)
		arg->usage();
	remoteaddr = dial->netmkaddr(hd args, "net", "venti");
	if(len args == 2)
		proxyaddr = dial->netmkaddr(hd tl args, "net", "venti");
	if(wflag && proxyaddr == nil)
		arg->usage();
	vflag += dflag;

	laddr = dial->netmkaddr(laddr, "tcp", "venti");
	ac := dial->announce(laddr);
	if(ac == nil)
		fail(sprint("announce %s: %r", laddr));
	vsay(sprint("announced to %s", laddr));

	wrotec = chan of (int, int);
	registerc = chan of (int, int, chan of ref Vmsg);
	requestc = chan of (ref Vmsg, int);
	remote = Conn.mk(remoteaddr);
	proxy = Conn.mk(proxyaddr);

	cacheheads = array[1+maxcachesize/(Dataavgsize*Blocksperhead)] of ref Block;
	say(sprint("have %d cacheheads", len cacheheads));

	spawn main();
	spawn listen(ac);
}

listen(ac: ref Dial->Connection)
{
	for(;;) {
		c := dial->listen(ac);
		if(c == nil)
			fail(sprint("listen: %r"));
		say("listening");
		say("have connection");
		spawn lreader(c);
	}
}

index(s: Score): int
{
	v := int s.a[0]<<16;
	v |= int s.a[1]<<8;
	v |= int s.a[2]<<0;
	return v % len cacheheads;
}

cacheget(s: Score, dtype: int): array of byte
{
	cachereq++;
	for(b := cacheheads[index(s)]; b != nil; b = b.looknext)
		if(b.dtype == dtype && b.s.eq(s)) {
			if(b.used < byte 2)
				b.used += byte 1;
			return b.d;
		}
	cachemiss++;
	return nil;
}

cacheput(s: Score, dtype: int, d: array of byte)
{
	need := Blocksize+len d;
	if(need > maxcachesize)
		return;
	while(take != nil && cacheused+need > maxcachesize) {
		if(int take.used) {
			take.used -= byte 1;
			take = take.takenext;
			continue;
		}
		ti := index(take.s);
		if(take.lookprev == nil)
			cacheheads[ti] = take.looknext;
		else
			take.lookprev.looknext = take.looknext;
		if(take.looknext != nil)
			take.looknext.lookprev = take.lookprev;

		cacheused -= Blocksize+len take.d;
		if(take.takenext == take) {
			take = nil;
		} else {
			take.takeprev.takenext = take.takenext;
			take.takenext.takeprev = take.takeprev;
			take = take.takenext;
		}
	}

	i := index(s);
	b := ref Block(s, dtype, d, byte 0, nil, nil, nil, nil);
	b.looknext = cacheheads[i];
	cacheheads[i] = b;
	if(b.looknext != nil)
		b.looknext.lookprev = b;
	if(take == nil) {
		b.takeprev = b.takenext = b;
	} else {
		b.takenext = take;
		b.takeprev = take.takeprev;
		take.takeprev = b;
		take.takenext.takeprev = b;
	}
	take = b;
	cacheused += need;
}


Toremote, Nothing, Claimremote, Claimproxy: con 1<<iota;
Op: adt {
	ctid:	int;	# tag from client
	c:	ref Client;
	wait:	int;
	err:	string;
	flags:	int;
	s:	ref Score;
	dtype,
	dsize:	int;
};

Client: adt {
	rpid,
	wpid:	int;
	respc:	chan of ref Vmsg;
	ntid:	int;	# number of active tids
};

Conn: adt {
	fd:		ref Sys->FD;
	addr:		string;
	tids:		list of int;
	pending:	array of ref Op;
	rpid,
	wpid:	int;
	claimed:	int;
	inc,
	writec:		chan of ref Vmsg;
	errorc:		chan of int;

	mk:		fn(addr: string): ref Conn;
	dial:		fn(c: self ref Conn): int;
	tidsfree:	fn(c: self ref Conn): int;
	optake:		fn(c: self ref Conn, tid: int): ref Op;
	opput:		fn(c: self ref Conn, op: ref Op): int;
	clear:		fn(c: self ref Conn, cc: ref Client);
	close:		fn(c: self ref Conn, cc: ref Conn);
};


ventidial(addr: string): ref Sys->FD
{
	c := dial->dial(addr, nil);
	if(c == nil)
		return nil;
	session := Session.new(c.dfd);
	if(session == nil)
		return nil;
	return c.dfd;
}

Conn.mk(addr: string): ref Conn
{
	inc := chan of ref Vmsg;
	writec := chan[256] of ref Vmsg;
	errorc := chan of int;
	return ref Conn(nil, addr, nil, array[256] of ref Op, 0, 0, 0, inc, writec, errorc);
}

Conn.dial(c: self ref Conn): int
{
	c.fd = ventidial(c.addr);
	if(c.fd == nil)
		return -1;
	c.tids = nil;
	for(i := 255; i >= 0; i--)
		c.tids = i::c.tids;
	pidc := chan of int;
	spawn vreader(pidc, c.fd, c.inc);
	c.rpid = <- pidc;
	spawn vwriter(pidc, c.fd, c.writec, c.errorc);
	c.wpid = <- pidc;
	return 0;
}

Conn.tidsfree(c: self ref Conn): int
{
	ntids := 256;
	if(c.fd != nil)
		ntids = len c.tids;
	return ntids - c.claimed;
}

Conn.optake(c: self ref Conn, tid: int): ref Op
{
	op := c.pending[tid];
	c.pending[tid] = nil;
	c.tids = tid::c.tids;
	return op;
}

Conn.opput(c: self ref Conn, op: ref Op): int
{
	tid := hd c.tids;
	c.tids = tl c.tids;
	c.pending[tid] = op;
	return tid;
}

Conn.clear(c: self ref Conn, cc: ref Client)
{
	for(i := 0; i < len c.pending; i++)
		if(c.pending[i] != nil && c.pending[i].c == cc) {
			op := c.pending[i];
			c.pending[i] = nil;
			unclaim(op, Claimremote|Claimproxy);
		}
}

Conn.close(c: self ref Conn, cc: ref Conn)
{
	c.fd = nil;
	if(c.rpid != 0)
		kill(c.rpid);
	if(c.wpid != 0)
		kill(c.wpid);
	c.rpid = c.wpid = 0;
	closec: list of ref Client;
	for(i := 0; i < len c.pending; i++) {
		op := c.pending[i];
		c.pending[i] = nil;
		if(op != nil)
			unclaim(op, Claimremote|Claimproxy);
		if(op == nil || op.c == nil || clientget(op.c.rpid) == nil)
			continue;
		clientdel(op.c);
		closec = op.c::closec;
	}
	for(; closec != nil; closec = tl closec) {
		client := hd closec;
		for(i = 0; cc != nil && i < len cc.pending; i++) {
			op := cc.pending[i];
			if(op != nil && op.c == client) {
				cc.pending[i] = nil;
				unclaim(op, Claimremote|Claimproxy);
			}
		}
		clientkill(client);
	}
}

clientget(rpid: int): ref Client
{
	for(l := clients; l != nil; l = tl l)
		if((hd l).rpid == rpid)
			return hd l;
	return nil;
}

clientput(rpid, wpid: int, respc: chan of ref Vmsg)
{
	clients = ref Client(rpid, wpid, respc, 0)::clients;
}

clientdel(c: ref Client)
{
	l: list of ref Client;
	for(; clients != nil; clients = tl clients)
		if(c != hd clients)
			l = hd clients::l;
	clients = l;
}

clientkill(c: ref Client)
{
	kill(c.rpid);
	kill(c.wpid);
}

killclient(c: ref Client)
{
	remote.clear(c);
	proxy.clear(c);
	clientkill(c);
	clientdel(c);
}

needconn(c: ref Conn, cc: ref Client): int
{
	if(c.fd != nil)
		return 0;
	vsay(sprint("needconn: dialing %s", c.addr));
	if(c.dial() < 0) {
		vsay(sprint("needconn: dial failed: %r"));
		if(cc != nil)
			killclient(cc);
		return -1;
	}
	vsay("needconn: dial okay");
	return 0;
}

sha1(d: array of byte): array of byte
{
	r := array[kr->SHA1dlen] of byte;
	kr->sha1(d, len d, r, nil);
	return r;
}

opokay(op: ref Op, vmsg: ref Vmsg): int
{
	pick msg := vmsg {
	Rread =>
		rscore := Score(sha1(msg.data));
		return rscore.eq(*op.s);
	Rwrite =>
vsay(sprint("opokay, msg.score=%s op.s nil==%d", msg.score.text(), op.s==nil));
		return msg.score.eq(*op.s);
	};
	return 1;
}

unclaim(op: ref Op, which: int)
{
	if(which & op.flags & Claimremote) {
		remote.claimed--;
		op.flags &= ~Claimremote;
	}
	if(which & op.flags & Claimproxy) {
		proxy.claimed--;
		op.flags &= ~Claimproxy;
	}
}

tick(c: chan of int)
{
	for(;;) {
		sys->sleep(10*1000);
		c <-= 0;
	}
}

main()
{
	bogusreqc := chan of (ref Vmsg, int);
	reqc := requestc;

	fio := sys->file2chan(statsdir, statsfile);
	if(fio == nil) {
		sys->fprint(sys->fildes(2), "file2chan: %r;  not serving statistics\n");
		fio = ref sys->FileIO(chan of (int, int, int, sys->Rread), chan of (int, array of byte, int, sys->Rwrite));
	} else
		if(dflag) say(sprint("file2chan: serving %s%s", statsdir, statsfile));

	say("main: beginning loop");
loop:
	for(;;) {
		reqc = bogusreqc;
		if(remote.tidsfree() > 0 && proxy.tidsfree() > 0)
			reqc = requestc;
		if(clients == nil && remote.fd != nil) {
			remote.close(nil);
			say("main: remote closed");
		}

		if(dflag) say(sprint("main: ALT rclaim=%d pclaim=%d rfree=%d pfree=%d",
			remote.claimed, proxy.claimed, remote.tidsfree(), proxy.tidsfree()));
		alt {
		(offset, nil, nil, rc) := <- fio.read =>
			if(rc == nil)
				continue;

			buf := sys->aprint(
				"%14d clients\n%14d proxy connection\n%14d proxy transitops\n%14d remote connection\n%14d remote transitops\n"+
				"%14d maxcachesize\n%14d cacheheads\n%14d cacheused\n"+
				"%14d cachemiss\n%14d cachehit\n%14d cacherequest\n"+
				"%14d proxymiss\n%14d proxyhit\n%14d proxyrequest\n"+
				"%14d remotereads\n%14d remotewrites\n",
				len clients, proxy.fd != nil, 256-proxy.tidsfree(), remote.fd != nil, 256-remote.tidsfree(),
				maxcachesize, len cacheheads, cacheused,
				cachemiss, cachereq-cachemiss, cachereq,
				proxymiss, proxyreq-proxymiss, proxyreq,
				remotereads, remotewrites);

			if(offset > len buf)
				offset = len buf;
			rc <-= (buf[offset:], nil);

		(nil, nil, nil, wc) := <- fio.write =>
			if(wc == nil)
				continue;
			if(dflag) say("main: file2chan write");
			wc <-= (0, Eperm);

		(rpid, wpid, respc) := <- registerc =>
			vsay("main: new client");
			clientput(rpid, wpid, respc);

		(vmsg, rpid) := <- reqc =>
			if(dflag) say(sprint("main: request from rpid=%d", rpid));
			c := clientget(rpid);
			if(vmsg == nil) {
				vsay("main: error from lreader");
				killclient(c);
				continue loop;
			}

			if(c.ntid == 256) {
				vsay("main: bad client, already has 256 tids in use!");
				killclient(c);
				continue loop;
			}
			c.ntid++;

			if(tagof vmsg == tagof Vmsg.Tping) {
				c.respc <-= ref Vmsg.Rping(0, vmsg.tid);
				continue loop;
			}
				
			if(dflag) say("main: before cache");
			pick tmsg := vmsg {
			Tread =>
				d := cacheget(tmsg.score, tmsg.etype);
				if(d != nil) {
					c.respc <-= ref Vmsg.Rread(0, vmsg.tid, d);
					if(dflag) say("main: cache hit");
					continue loop;
				}
				if(dflag) say("main: cache miss");
			}
			if(dflag) say("main: not in cache");

			score: ref Score;
			dtype, dsize: int;
			isread := 0;
			pick tmsg := vmsg {
			Tread =>
				isread = 1;
				score = ref tmsg.score;
				dtype = tmsg.etype;
				dsize = tmsg.n;
			Twrite =>
				score = ref Score(sha1(tmsg.data));
			}
			op: ref Op;
			if(proxyaddr != nil && (isread || wflag)) {
				if(dflag) say("main: using proxy connection");
				cc: ref Client;
				if(wflag)
					cc = c;
				if(needconn(proxy, cc) < 0) {
					if(wflag)
						continue loop;
					if(dflag) say("main: proxy cache not available, continuing");
				} else {
					op = ref Op(vmsg.tid, c, 1, nil, 0, score, dtype, dsize);
					vmsg.tid = proxy.opput(op);
					proxy.writec <-= vmsg;
					if(isread) {
						op.flags |= Toremote|Claimremote;
						remote.claimed++;
						continue loop;
					}
					if(dflag) say("main: continuing after proxy");
				}
			}

			if(dflag) say("main: using remote connection");
			if(needconn(remote, c) < 0)
				continue loop;

			if(op == nil)
				op = ref Op(vmsg.tid, c, 0, nil, 0, score, dtype, dsize);
			op.wait++;
			vmsg.tid = remote.opput(op);
			if(dflag) say(sprint("main: rtid=%d", vmsg.tid));
			remote.writec <-= vmsg;
			case tagof vmsg {
			tagof Vmsg.Tread =>	remotereads++;
			tagof Vmsg.Twrite =>	remotewrites++;
			}
			if(dflag) say("main: client request written to remote");

		(rpid, ok) := <- wrotec =>
			if(dflag) say("main: local writer wrote msg");
			c := clientget(rpid);
			if(c != nil) {
				if(ok == 0)
					killclient(c);
				else
					c.ntid--;
			}

		vmsg := <- proxy.inc =>
			if(dflag) say("main: vmsg from proxy");
			if(vmsg == nil) {
				vsay("main: proxy read error");
				proxy.close(remote);
				continue loop;
			}

			op := proxy.optake(vmsg.tid);
			if(op == nil)
				continue loop;
			unclaim(op, Claimremote);
			op.wait--;
			if(!nflag && !opokay(op, vmsg)) {
				vsay("main: proxy sent bad data");
				if(op.wait == 0)
					op.c.respc <-= ref Vmsg.Rerror(0, op.ctid, "proxy sent bad data");
				else
					op.err = "proxy sent bad data";
				continue loop;
			}
			pick rmsg := vmsg {
			Rread =>
				cacheput(*op.s, op.dtype, rmsg.data);
			}
			if(op.flags & Nothing) {
				if(dflag) say("main: op says nothing to do");
				continue loop;
			}
			proxyreq++;
			if(tagof vmsg == tagof Vmsg.Rerror && op.flags&Toremote) {
				proxymiss++;
				if(dflag) say("main: have error from proxy, sending to remote");
				if(needconn(remote, op.c) < 0)
					continue loop;
				op.flags |= Claimproxy;
				proxy.claimed++;
				rtid := remote.opput(op);
				op.wait++;
				remote.writec <-= ref Vmsg.Tread(0, rtid, *op.s, op.dtype, op.dsize);
				remotereads++;
				if(dflag) say("main: resent to remote");
				continue loop;
			}
			pick rmsg := vmsg {
			Rerror =>	op.err = rmsg.e;
			}
			if(op.wait == 0) {
				if(op.err != nil)
					vmsg = ref Vmsg.Rerror(0, 0, op.err);
				vmsg.tid = op.ctid;
				op.c.respc <-= vmsg;
				if(dflag) say("main: proxy op handled");
			}
			if(dflag) say("main: proxy read handled");

		<- proxy.errorc =>
			vsay("main: proxy write error");
			proxy.close(remote);

		vmsg := <- remote.inc =>
			if(dflag) say("main: vmsg from remote");
			if(vmsg == nil) {
				if(vflag) vsay("main: remote read error");
				remote.close(proxy);
				continue loop;
			}

			if(dflag) say(sprint("main: vmsg.tid=%d", vmsg.tid));
			op := remote.optake(vmsg.tid);
			if(op == nil) {
				if(dflag) say("main: op == nil");
				continue loop;
			}
			unclaim(op, Claimproxy);
			op.wait--;
			if(!nflag && !opokay(op, vmsg)) {
				if(dflag) say("main: remote sent bad data");
				if(op.wait == 0)
					op.c.respc <-= ref Vmsg.Rerror(0, op.ctid, "remote sent bad data");
				else
					op.err = "remote sent bad data";
				continue loop;
			}
			isread := 0;
			pick r := vmsg {
			Rread =>
				cacheput(*op.s, op.dtype, r.data);
				isread = 1;
				if(proxyaddr != nil && needconn(proxy, nil) >= 0) {
					ptid := proxy.opput(ref Op(0, nil, 0, nil, Nothing, op.s, 0, 0));
					proxy.writec <-= ref Vmsg.Twrite(1, ptid, op.dtype, r.data);
					if(dflag) say("main: wrote data from remote to proxy");
				}
			Rerror =>
				op.err = r.e;
			}
			if(op.wait == 0) {
				if(op.err != nil)
					vmsg = ref Vmsg.Rerror(0, 0, op.err);
				vmsg.tid = op.ctid;
				op.c.respc <-= vmsg;
				if(dflag) say("main: op done after remote read");
			}
			if(dflag) say("main: remote read okay");

		<- remote.errorc =>
			vsay("main: remote writer error");
			remote.close(proxy);
		}
	}
}

readline(fd: ref Sys->FD): array of byte
{
	buf := array[128] of byte;
	for(i := 0; i < len buf; i++)
		case sys->read(fd, buf[i:], 1) {
		0 =>
			sys->werrstr("eof");
			return nil;
		1 =>
			if(buf[i] == byte '\n')
				return buf[:i];
		* =>
			return nil;
		}
	sys->werrstr("version line too long");
	return nil;
}

handshake(fd: ref Sys->FD): string
{
	if(sys->fprint(fd, "venti-02-vcache\n") < 0)
		return sprint("writing version: %r");

	d := readline(fd);
	if(d == nil)
		return sprint("bad version (%r)");
	if(!str->prefix("venti-02-", string d))
		return sprint("bad version (%s)", string d);

	(tvmsg, terr) := Vmsg.read(fd);
	if(terr != nil)
		return "reading thello: "+terr;

	if(tagof tvmsg != tagof Vmsg.Thello)
		return "first message not thello";

	rvmsg := ref Vmsg.Rhello(0, 0, nil, 0, 0);
	md := rvmsg.pack();
	if(md == nil || sys->write(fd, md, len md) != len md)
		return sprint("writing rhello: %r");
	return nil;
}

lreader(c: ref dial->Connection)
{
	fd := dial->accept(c);
	if(fd == nil)
		return vsay(sprint("accept: %r"));
	say("lreader: starting");
	herr := handshake(fd);
	if(herr != nil)
		return vsay(sprint("lreader: handshake: %s", herr));
	say("lreader: have handshake");

	rpid := sys->pctl(0, nil);
	spawn lwriter(fd, rpid, pidc := chan of int, respc := chan[256] of ref Vmsg);
	wpid := <- pidc;

	registerc <-= (rpid, wpid, respc);

	for(;;) {
		(vmsg, err) := Vmsg.read(fd);
		if(vmsg != nil && !vmsg.istmsg)
			err = "message not tmsg";
		if(vmsg != nil && tagof vmsg == tagof Vmsg.Tgoodbye)
			err = "lreader closing down";
		if(err != nil || vmsg == nil) {
			vsay("lreader: reading: "+err);
			requestc <-= (nil, rpid);
			break;
		}
		requestc <-= (vmsg, rpid);
	}
}

lwriter(fd: ref Sys->FD, rpid: int, pidc: chan of int, respc: chan of ref Vmsg)
{
	pidc <-= sys->pctl(0, nil);
	for(;;) {
		vmsg := <- respc;
		if(vmsg == nil)
			break;
		d := vmsg.pack();
		if(sys->write(fd, d, len d) != len d) {
			vsay(sprint("lwriter: writing: %r"));
			wrotec <-= (rpid, 0);
			break;
		}
		wrotec <-= (rpid, 1);
	}
}

vreader(pidc: chan of int, fd: ref Sys->FD, inc: chan of ref Vmsg)
{
	pidc <-= sys->pctl(0, nil);
	for(;;) {
		(vmsg, err) := Vmsg.read(fd);
		if(vmsg != nil && vmsg.istmsg)
			err = "received tmsg, expected rmsg";
		if(err != nil) {
			vmsg = nil;
			vsay("vreader: "+err);
		}
if(vmsg != nil)
	if(dflag) say(sprint("vreader: have msg tid=%d tag=%d tagthello=%d", vmsg.tid, tagof vmsg, tagof Vmsg.Thello));
		inc <-= vmsg;
		if(vmsg == nil)
			break;
	}
}

vwriter(pidc: chan of int, fd: ref Sys->FD, outc: chan of ref Vmsg, errorc: chan of int)
{
	pidc <-= sys->pctl(0, nil);
	for(;;) {
		vmsg := <- outc;
		if(vmsg == nil)
			break;
if(dflag) say(sprint("vwriter: have msg tid=%d tag=%d tagthello=%d", vmsg.tid, tagof vmsg, tagof Vmsg.Thello));
		d := vmsg.pack();
		if(sys->write(fd, d, len d) != len d) {
			vsay(sprint("vwriter: writing: %r"));
			errorc <-= 0;
			break;
		}
	}
}

kill(pid: int)
{
	sys->fprint(sys->open(sprint("/prog/%d/ctl", pid), sys->OWRITE), "kill");
	vsay(sprint("killed pid %d", pid));
}

fd2: ref Sys->FD;
warn(s: string)
{
	if(fd2 == nil)
		fd2 = sys->fildes(2);
	sys->fprint(fd2, "%s\n", s);
}

vsay(s: string)
{
	if(vflag)
		warn(s);
}

say(s: string)
{
	if(dflag)
		warn(s);
}

fail(s: string)
{
	warn(s);
	raise "fail:"+s;
}
