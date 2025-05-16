This project implements the O2PL (Ordered Two Phase Locking) scheduler and compares it with SS2PL (Strong Two Phase Locking), BOCC (Backward Oriented Concurrency Control) and FOCC (Forward Oriented Concurrency Control) schedulers.

Members:

- [Aayush Kumar](https://github.com/Random-Bee)
- [Aaryan](https://github.com/aaryan200)

The detailed report of the project can be found [here](./ProjectReport.pdf).

# O2PL Variants

O2PL has 2 variants:
1. **O2PL with input from file**: Useful for checking the correctness of the algorithm on small inputs.
2. **O2PL with input BTO-Like scheduler**: This input method is common among the other schedulers and is used for comparing performance on large inputs.

---

## Compilation and Execution Instructions

### O2PL with File Input

To compile:

```bash
g++ Source-CO21BTECH11002-O2PL-FileInput.cpp -o O2PL_FileInput
```

To run the program:

```bash
./O2PL_FileInput <input_file>
```

The input file has the following format:

```
<thread id> <operation> <item id>
```

Where `<operation>` is either `r` or `w` (without quotes).

---

### O2PL with BTO-Like Scheduler

To compile:

```bash
g++ Source-CO21BTECH11002-O2PL.cpp -o O2PL
```

To run the program:

```bash
./O2PL <totalTrans> <numThreads> <numItems> <numIters> <writeProbab>
```

A sample execution command is:

```bash
./O2PL 5000 16 5000 20 0.2
```

Where:
- `<totalTrans>`: Total number of transactions to be executed.
- `<numThreads>`: Number of threads to be used.
- `<numItems>`: Number of items in the database.
- `<numIters>`: Number of iterations to be executed per transaction.
- `<writeProbab>`: Probability of write operation (between 0 and 1). For example, if you want 70% of the iterations to perform write operations, set `<writeProbab>` to `0.7`.

---

### SS2PL

To compile:

```bash
g++ Source-CO21BTECH11002-SS2PL.cpp -o SS2PL
```

To run the program:

```bash
./SS2PL <totalTrans> <numThreads> <numItems> <numIters> <writeProbab>
```

The arguments are the same as for O2PL.

---

### BOCC

To compile:

```bash
g++ Source-CO21BTECH11002-BOCC.cpp -o BOCC
```

To run the program:

```bash
./BOCC <totalTrans> <numThreads> <numItems> <numIters> <writeProbab>
```

The arguments are the same as for O2PL.

---

### FOCC

To compile:

```bash
g++ Source-CO21BTECH11002-FOCC.cpp -o FOCC
```

To run the program:

```bash
./FOCC <totalTrans> <numThreads> <numItems> <numIters> <writeProbab>
```

The arguments are the same as for O2PL.