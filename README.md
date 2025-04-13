# JDXT&JDXT+

The warehouse implements a dynamic equivalent join query solution, JDXT, which is both forward-secure and backward-secure. This solution can support multi-table join queries on a database that supports dynamic updates. To further enhance privacy, an enhanced version JDXT+ is provided. We show their construction and comparison.

## Background

The corresponding works JDXT and JDXT+ solve the dynamic join query on encrypted databases. This project aims to implement JDXT and JDXT+ in JAVA and compare them with JXT++ in terms of storage overhead and query efficiency.

## Features

- Applied Cryptography
- Encrypted Database
- Join Queries
- JAVA

### Test (No need Internet)

Test to run the java files

```
src/test/java/JDXT.java
src/test/java/JDXTEMM.java
src/test/java/JXTHJS.java
```

## Project structure

### File tree

```
MJXT/
├── pom.xml
├── README.md
├── data																								
├──	├── *										//Datasets
├── src
│   ├── main
│   │   └── java
│   │       ├── client																	
│   │       │   ├── Setup_JDXT.java				//Setup algorithm of JDXT
│   │       │   ├── Setup_JDXTEMM.java				//Setup algorithm of JDXTEMM
│   │       │   └── Setup_JDXTHJS.java			//Setup algorithm of JDXT+
│   │       ├── server
│   │       │   ├── Server_JDXT.java				//Search algorithm of JDXT
│   │       │   ├── Server_JDXTEMM.java			//Search algorithm of JDXTEMM
│   │       │   └── Server_JDXTHJS.java			//Search algorithm of JDXT+
│   │       └── utils
│   └── test
│       └── java
│           ├── JDXT.java						//JDXT update scheme
│           ├── JDXT_CSV.java						//JDXT scheme for bigdata
│           ├── JDXTEMM.java						//JDXTEMM update scheme
│           ├── JDXTEMM_CSV.java						//JDXTEMM scheme for bigdata
│           ├── JDXTHJS.java						//JDXT+ update scheme
│           ├── JDXTHJS_CSV.java					//JDXT+ sheme for bigdata
│           ├── test_del.java			//deletion test for JDXT and JDXT+
│           ├── test_del0.java				//deletion 0 test for JDXT and JDXT+
│           ├── test_join.java					//join tables test for JDXT and JDXTEMM
│           ├── test_storage.java					//test storage overhead for three schemes
│           ├── test_update.java					//different join-attributes test for three schemes 
│           └── test_wEntropy.java				//high entropy of attribute test for JDXT and JDXT+
└── target (generated after build)
```