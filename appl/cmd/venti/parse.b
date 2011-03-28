implement Ventiparse;

include "sys.m";
	sys: Sys;
	sprint: import sys;
include "draw.m";
include "string.m";
	str: String;
include "arg.m";
include "venti.m";
	venti: Venti;
	Score, Scoresize, Entrydir, Dirtype, Datatype, Pointertype0: import venti;
include "vac.m";
	vac: Vac;
	Root, Entry, Entrysize, Direntry, Metablock, Metaentry, Metablocksize, Metaentrysize: import vac;

Direntrymagic:  con 16r1c4d9072;
Metablockmagic: con 16r5656fc79;

Ventiparse: module {
	init:	fn(nil: ref Draw->Context, args: list of string);
};

init(nil: ref Draw->Context, args: list of string)
{
	sys = load Sys Sys->PATH;
	arg := load Arg Arg->PATH;
	str = load String String->PATH;
	venti = load Venti Venti->PATH;
	venti->init();
	vac = load Vac Vac->PATH;
	vac->init();

	arg->init(args);
	arg->setusage(arg->progname() + " [type]");
	while((c := arg->opt()) != 0)
		case c {
		* =>	arg->usage();
		}
	args = arg->argv();
	if(len args > 1)
		arg->usage();

	d := read();

	dtype := "";
	if(len args == 1)
		dtype = hd args;
	if(dtype == nil)
		dtype = guesstype(d);
	if(dtype == nil)
		fail("could not guess type");

	case str->tolower(dtype) {
	"vac" or "root" =>
		root(d);
	"entry" =>
		entry(d);
	"entries" =>
		entries(d);
	"direntry" =>
		direntry(d);
	"metablock" =>
		metablock(d);
	"dir" or "metablocks" =>
		metablocks(d);
	"pointers" =>
		pointers(d);
	"rpointers" =>
		rpointers(d);
	* =>
		sys->fprint(sys->fildes(2), "bad type: %s\n", hd args);
		arg->usage();
	}
}

g32(f: array of byte, i: int): int
{
	return (((((int f[i+0] << 8) | int f[i+1]) << 8) | int f[i+2]) << 8) | int f[i+3];
}

zeros(d: array of byte): int
{
	n := 0;
	for(i := 0; i < len d; i++)
		if(d[i] == byte 0)
			n++;
	return n;
}

guesstype(d: array of byte): string
{
	if(len d < 4)
		return nil;
	v := g32(d, 0);
	if(v == Direntrymagic)
		return "direntry";
	if(v == Metablockmagic)
		return "metablocks";
	if(len d == 300)
		return "root";

	nz := zeros(d);
	if(len d % Entrysize == 0 && nz > (8*len d/Entrysize))
		return "entries";
	if(len d % Scoresize == 0 && len d % (Scoresize+8) == 0) {
		if(nz > 2*len d/Scoresize)
			return "rpointers";
		return "pointers";
	}
	if(len d % Scoresize == 0)
		return "pointers";
	if(len d % (Scoresize+8) == 0)
		return "rpointers";
	return nil;
}

read(): array of byte
{
	fd0 := sys->fildes(0);
	d := array[0] of byte;
	buf := array[sys->ATOMICIO] of byte;
	for(;;) {
		n := sys->read(fd0, buf, len buf);
		if(n == 0)
			break;
		if(n < 0)
			fail(sprint("reading data: %r"));
		nd := array[len d+n] of byte;
		nd[:] = d;
		nd[len d:] = buf[:n];
		d = nd;
	}
	return d;
}

root(d: array of byte)
{
	r := Root.unpack(d);
	if(r == nil)
		fail(sprint("unpacking root: %r"));
	printroot(r);
}

printroot(r: ref Root)
{
	sys->print("Root:\n");
	sys->print("\tversion=%d\n", r.version);
	sys->print("\tname=%s\n", r.name);
	sys->print("\trtype=%s\n", r.rtype);
	sys->print("\tscore=%s\n", r.score.text());
	sys->print("\tblocksize=%d\n", r.blocksize);
	sys->print("\tprev=%s\n", (*r.prev).text());
	sys->print("venti/read %d %s | venti/parse entries\n", Dirtype, r.score.text());
}

entry(d: array of byte)
{
	e := Entry.unpack(d);
	if(e == nil)
		fail(sprint("unpacking entry: %r"));
	printentry(e);
}

entries(d: array of byte)
{
	if(len d % Entrysize != 0)
		fail(sprint("data (%d bytes) not multiple of Entrysize (%d bytes)", len d, Entrysize));
	for(i := 0; i+Entrysize <= len d; i += Entrysize)
		entry(d[i:i+Entrysize]);
}

printentry(e: ref Entry)
{
	sys->print("Entry:\n");
	sys->print("\tgen=%d\n", e.gen);
	sys->print("\tpsize=%d\n", e.psize);
	sys->print("\tdsize=%d\n", e.dsize);
	sys->print("\tflags=%d\n", e.flags);
	sys->print("\tdepth=%d\n", e.depth);
	sys->print("\tsize=%bd\n", e.size);
	sys->print("\tscore=%s\n", e.score.text());
	if(e.depth > 0) {
		which := "pointers";
		dtype := Pointertype0+e.depth-1;
		sys->print("venti/read %d %s | venti/parse %s\n", dtype, e.score.text(), which);
	} else {
		dtype := Datatype;
		pipe := "";
		if(e.flags & Entrydir) {
			dtype = Dirtype;
			pipe = " | venti/parse entries";
		}
		sys->print("venti/read %d %s%s\n", dtype, e.score.text(), pipe);
	}
}

direntry(d: array of byte)
{
	de := Direntry.unpack(d);
	if(de == nil)
		fail(sprint("unpacking direntry: %r"));
	printdirentry(de);
}

printdirentry(de: ref Direntry)
{
	sys->print("Direntry:\n");
	sys->print("\tversion=%d\n", de.version);
	sys->print("\telem=%s\n", de.elem);
	sys->print("\tentry=%d\n", de.entry);
	sys->print("\tgen=%d\n", de.gen);
	sys->print("\tmentry=%d\n", de.mentry);
	sys->print("\tmgen=%d\n", de.mgen);
	sys->print("\tqid=%bux\n", de.qid);
	sys->print("\tuid=%s\n", de.uid);
	sys->print("\tgid=%s\n", de.gid);
	sys->print("\tmid=%s\n", de.mid);
	sys->print("\tmtime=%d\n", de.mtime);
	sys->print("\tmcount=%d\n", de.mcount);
	sys->print("\tctime=%d\n", de.ctime);
	sys->print("\tatime=%d\n", de.atime);
	sys->print("\tmode=%x\n", de.mode);
#	if(de.qidspace) {
#		sys->print("\tqidoff=%bd\n", de.qidoff);
#		sys->print("\tqidmax=%bd\n", de.qidmax);
#	}
}

metablock(d: array of byte)
{
	mb := Metablock.unpack(d);
	if(mb == nil)
		fail(sprint("unpacking metablock: %r"));
	printmetablock(mb, d, 0);
}

metablocks(d: array of byte)
{
	i := 0;
	while(i < len d) {
		mb := Metablock.unpack(d[i:]);
		if(mb == nil)
			fail(sprint("unpacking metablock: %r"));
		printmetablock(mb, d[i:], 1);
		i += Vac->Dsize;
	}
}

printmetablock(mb: ref Metablock, d: array of byte, printde: int)
{
	sys->print("Metablock:\n");
	sys->print("\tsize=%d\n", mb.size);
	sys->print("\tfree=%d\n", mb.free);
	sys->print("\tmaxindex=%d\n", mb.maxindex);
	sys->print("\tnindex=%d\n", mb.nindex);

	sys->print("Meta entries:\n");
	for(i := 0; i < mb.nindex; i++) {
		me := Metaentry.unpack(d, i);
		if(me == nil)
			fail(sprint("parsing meta entry: %r"));
		sys->print("\toffset=%d size=%d\n", me.offset, me.size);
	}

	if(!printde)
		return;

	for(i = 0; i < mb.nindex; i++) {
		me := Metaentry.unpack(d, i);
		if(me == nil)
			fail(sprint("parsing meta entry: %r"));
		de := Direntry.unpack(d[me.offset:me.offset+me.size]);
		if(de == nil)
			fail(sprint("parsing direntry: %r"));
		printdirentry(de);
	}
}

pointers(d: array of byte)
{
	if(len d % Scoresize != 0)
		fail(sprint("data not multiple of scoresize: %d", len d));
	sys->print("Pointers:\n");
	for(o := Scoresize; o <= len d; o += Scoresize)
		sys->print("\t%s\n", Score(d[o-Scoresize:o]).text());
}

rpointers(d: array of byte)
{
	esize := Scoresize+8;
	if(len d % esize != 0)
		fail(sprint("data not multiple of scoresize+8: %d", len d));
	sys->print("Rpointers:\n");
	for(o := 0; o+esize <= len d; o += esize)
		sys->print("\tlength=%10bd %s\n", g64(d, o+Scoresize), Score(d[o:o+Scoresize]).text());
}

g64(d: array of byte, i: int): big
{
	o := i;
	v := big 0;
	v |= big (d[o++]<<56);
	v |= big (d[o++]<<48);
	v |= big (d[o++]<<40);
	v |= big (d[o++]<<32);
	v |= big (d[o++]<<24);
	v |= big (d[o++]<<16);
	v |= big (d[o++]<<8);
	v |= big (d[o++]<<0);
	return v;
}

fail(s: string)
{
	sys->fprint(sys->fildes(2), "%s\n", s);
	raise "fail:"+s;
}
