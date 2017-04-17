A = LOAD '$inputfile';
B = FOREACH A GENERATE flatten(TOKENIZE((chararray)$0)) AS word;
C = GROUP B BY word;
D = FOREACH C generate COUNT(B), group;
store D into '$outputfile';
