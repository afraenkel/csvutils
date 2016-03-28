# CSV processing utilities in golang

concurrent csv processing using maps.

Given an input csv file:

```
in1,in2,in3,in4
a1 ,a2 ,a3 ,a4
...

```

Specify the transformation:

```
m := Mapping{
Inhdr:  ["in1", "in2", "in3", "in4"],
Outhdr: ["out1", "out2"],
Mapper: {"out1": f1, "out2": f2}
}
```

For tranformation functions:

```
f1(rec, []string){ return rec[0] + rec[1] }
f2(rec, []string){ return rec[0] + rec[2] }
```

which outputs:

```
out1,out2
a1a2,a1a3
...
```
