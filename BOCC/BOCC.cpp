#include <bits/stdc++.h>
#include <unistd.h>
using namespace std;
using namespace chrono;
typedef long long ll;

enum class Operation {
    READ,
    WRITE,
    COMMIT
};

// Enum for COMMIT and ABORT
enum class Status
{
    COMMIT,
    ABORT
};

mutex log_mtx;
ofstream logFile;

ll getCurTime() {
    return duration_cast<microseconds>(chrono::high_resolution_clock::now().time_since_epoch()).count();
}

void logEvent(ll transId, ll itemId, Operation op) {
    ll currTimeLocal = getCurTime();

    log_mtx.lock();

    if (op == Operation::READ) {
        logFile << "Transaction " << transId << " reads item " << itemId << " at time " << currTimeLocal << endl;
    }
    else if (op == Operation::WRITE) {
        logFile << "Transaction " << transId << " writes item " << itemId << " at time " << currTimeLocal << endl;
    }
    else {
        logFile << "Transaction " << transId << " commits at time " << currTimeLocal << endl;
    }
    log_mtx.unlock();
}

// Transaction class
class Transaction {
public:
    ll id;
    set<ll> read_set;
    set<ll> write_set;
    map<ll, ll> write_vals; // Map from item index to new value
    long long startTime;
    long long endTime;

    Transaction(ll id) {
        this->id = id;
        startTime = getCurTime();
    }
};
    
// Item class
class Item {
    ll val;
    mutex lck;
    
public:
    set<pair<long long, ll>> write_list; // Set of {endTime, transId} of the transactions that performed write on the item

    Item() {
        val = 0;
    }

    void lock() {
        lck.lock();
    }

    void unlock() {
        lck.unlock();
    }

    ll get_val() {
        return val;
    }

    void set_val(ll new_val) {
        val = new_val;
    }
};
    
// BOCC class
class BOCC {
private:
    vector<Item*> db; // Database

    void garbageCollect(set<ll>& readWriteUnion) {
        // The transaction has already acquired the locks for all items in read-write union and the activeTransStartTime_mtx lock
        // Remove the transactions whose endTime is less than the minimum startTime of the active transactions

        long long minStartTime = activeTransStartTime.begin()->first;

        for (auto& item_idx: readWriteUnion) {
            vector<pair<long long, ll>> toRemove;

            for (auto& t: db[item_idx]->write_list) {
                long long endTime = t.first;
                if (endTime < minStartTime) {
                    toRemove.push_back(t);
                }
            }

            for (auto& t: toRemove) {
                db[item_idx]->write_list.erase(t);
            }
        }
    }

    void cleanup(Transaction* trans, set<ll>& readWriteUnion) {
        // Acquire the lock for the active transactions set
        activeTransStartTime_mtx.lock();

        // Garbage collection before terminating the transaction
        garbageCollect(readWriteUnion);

        // Remove itself from the set of active transactions
        activeTransStartTime.erase({trans->startTime, trans->id});

        activeTransStartTime_mtx.unlock();

        // Release the locks
        for (auto& item_idx: readWriteUnion) {
            db[item_idx]->unlock();
        }
    }
    
public:
    atomic<ll> ctr; // Counter for transaction id
    set<pair<long long, ll>> activeTransStartTime; // Set of {startTime, transId} of the active transactions
    mutex activeTransStartTime_mtx;

    BOCC(ll size) {
        ctr.store(1);
        db.resize(size, nullptr);
        for (int i = 0; i < size; i++) {
            db[i] = new Item();
        }
    }

    ~BOCC() {
        for (int i = 0; i < db.size(); i++) {
            delete db[i];
        }
    }

    Transaction* begin_trans() {
        ll id = ctr.fetch_add(1);

        Transaction* t = new Transaction(id);

        // Add the transaction to the set of active transactions
        activeTransStartTime_mtx.lock();

        activeTransStartTime.insert({t->startTime, id});

        activeTransStartTime_mtx.unlock();

        return t;
    }

    void read(Transaction* trans, ll item_idx, ll &localVal) {

        // If the item is already present in write set of the transaction, read the value it has written in local
        if (trans->write_set.find(item_idx) != trans->write_set.end()) {
            localVal = trans->write_vals[item_idx];
            return;
        }

        // Acquire the lock for the database item
        db[item_idx]->lock();

        localVal = db[item_idx]->get_val();

        // Release the lock
        db[item_idx]->unlock();

        // Add item to read set of the transaction
        trans->read_set.insert(item_idx);
    }

    void write(Transaction* trans, ll item_idx, ll newVal) {
        // Add item to write set of the transaction
        trans->write_set.insert(item_idx);

        // Store the new value in the transaction's write
        trans->write_vals[item_idx] = newVal;
    }

    Status tryCommit(Transaction* trans) {
        set<ll> readWriteUnion; // Union of read set and write set of the transaction

        for (auto& item_idx: trans->read_set) {
            readWriteUnion.insert(item_idx);
        }

        for (auto& item_idx: trans->write_set) {
            readWriteUnion.insert(item_idx);
        }

        // Acquire the locks for all items in read-write union
        for (auto& item_idx: readWriteUnion) {
            db[item_idx]->lock();
        }

        // Begin validation phase
        // Iterate through the write list of each item in read set of the transaction
        for (auto& item_idx: trans->read_set) {
            for (auto& t: db[item_idx]->write_list) {
                long long endTime = t.first;
                if (endTime < trans->startTime) {
                    continue;
                }
                else {
                    // RS(tj) ∩ WS(ti) is not null
                    // Abort the transaction

                    cleanup(trans, readWriteUnion);

                    return Status::ABORT;
                }
            }
        }

        // Transaction validated
        trans->endTime = getCurTime();

        // Write on the database
        for (auto& [idx, val]: trans->write_vals) {
            db[idx]->set_val(val);

            logEvent(trans->id, idx, Operation::WRITE);

            // Add the transaction to the write list of the item
            db[idx]->write_list.insert({trans->endTime, trans->id});
        }

        cleanup(trans, readWriteUnion);

        return Status::COMMIT;
    }
};

BOCC* bocc = nullptr;

ll totalTrans, numThreads, numItems, numIters;
double writeProbab;
vector<mutex*> item_locks;
vector<ll> maxReadScheduled, maxWriteScheduled;
atomic<ll> num_item_accessed;

bool canRead(ll item_id, ll transId) {
    // Check if the transaction can read the item
    if (transId < maxWriteScheduled[item_id]) {
        return false;
    }
    return true;
}

bool canWrite(ll item_id, ll transId) {
    // Check if the transaction can write to the item
    if (transId < max(maxReadScheduled[item_id], maxWriteScheduled[item_id])) {
        return false;
    }
    return true;
}

void work(ll tid) {
    ll numTrans = totalTrans / numThreads + (tid < totalTrans % numThreads);

    // Initialize the random number generator with threadId and time as seed
    unsigned seed = static_cast<unsigned>(tid) * static_cast<unsigned>(time(nullptr));

    // Initialize the random number generator
    default_random_engine random_number_generator(seed);

    uniform_int_distribution<ll> unifRand_idx(0, numItems - 1); // For random index
    uniform_int_distribution<ll> unifRand_val(0, 100); // For random value
    bernoulli_distribution writeDist(writeProbab); // For write probability

    for (ll i = 0; i < numTrans; i++) {
        Transaction* t = bocc->begin_trans();

        // Choose numIters random items to be updated
        unordered_set<ll> randIndices;
        while (randIndices.size() < numIters) {
            ll randInd = unifRand_idx(random_number_generator);
            randIndices.insert(randInd);
        }

        for (auto& randInd : randIndices) {
            ll locVal;

            // Lock the item
            item_locks[randInd]->lock();

            if (!canRead(randInd, t->id)) {
                item_locks[randInd]->unlock();
                continue;
            }

            // Read the value of the item in locVal
            bocc->read(t, randInd, locVal);

            logEvent(t->id, randInd, Operation::READ);

            num_item_accessed++;

            // Update maxReadScheduled
            maxReadScheduled[randInd] = max(maxReadScheduled[randInd], t->id);

            // Unlock the item
            item_locks[randInd]->unlock();
            
            bool write = writeDist(random_number_generator);

            if (write) {
                // Lock the item
                item_locks[randInd]->lock();

                if (!canWrite(randInd, t->id)) {
                    item_locks[randInd]->unlock();
                    continue;
                }

                // Update the local value
                locVal += unifRand_val(random_number_generator);

                // Write the new value to the item
                bocc->write(t, randInd, locVal);

                // Update maxWriteScheduled
                maxWriteScheduled[randInd] = max(maxWriteScheduled[randInd], t->id);

                // Unlock the item
                item_locks[randInd]->unlock();
            }
        }

        // Try to commit the transaction
        bocc->tryCommit(t);

        logEvent(t->id, -1, Operation::COMMIT);

        delete t;
    }
}

int main(int argc, char* argv[]) {

    if (argc != 6) {
        cout << "Usage: " << argv[0] << " <totalTrans> <numThreads> <numItems> <numIters> <writeProbab>" << endl;
        return 1;
    }

    // Read from command line
    totalTrans = stoll(argv[1]);
    numThreads = stoll(argv[2]);
    numItems = stoll(argv[3]);
    numIters = stoll(argv[4]);
    writeProbab = stod(argv[5]);

    bocc = new BOCC(numItems);

    item_locks.resize(numItems, new mutex());
    maxReadScheduled.resize(numItems, 0);
    maxWriteScheduled.resize(numItems, 0);
    num_item_accessed = 0;

    // Initialize the log file
    logFile.open("BOCC-log.txt");

    vector<thread> threads;

    ll startTime = getCurTime();

    for(ll i=0; i < numThreads; i++) {
        threads.push_back(thread(work, i));
    }

    for(ll i=0; i < numThreads; i++) {
        threads[i].join();
    }

    ll endTime = getCurTime();

    double avgCommitDelay = (double)(endTime - startTime) / (double)totalTrans;

    printf("Average time taken to commit a transaction: %.3lf microseconds\n", avgCommitDelay);

    double avg_item_accessed = (double)num_item_accessed / (double)totalTrans;
    printf("Average number of items accessed per transaction: %.3lf\n", avg_item_accessed);
    printf("Total number of items accessed: %lld\n", num_item_accessed.load());

    logFile.close();

    delete bocc;

    return 0;
}