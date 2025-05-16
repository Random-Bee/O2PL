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

class Transaction {
public:
    ll id;
    // // Map of item id to pair of operatin id and operation type
    map<ll, vector<pair<ll, Operation>>> operations;

    Transaction(ll id) {
        this->id = id;
    }
};

class Item {
public:
    atomic<ll> read_op_ctr, write_op_ctr, read_item_ctr, write_item_ctr;
    atomic<ll> read_ulock_item_ctr, write_ulock_item_ctr;
    ll val;
    Item() {
        read_op_ctr = 0;
        write_op_ctr = 0;
        read_item_ctr = 0;
        write_item_ctr = 0;

        read_ulock_item_ctr = 0;
        write_ulock_item_ctr = 0;
        
        val = 0;
    }
};

class O2PL {
private:

    vector<Item*> items;
    ll size;
    atomic<ll> trans_id_ctr;

    ll get_op_ctr(ll item_id, Operation op) {
        ll op_ctr;
        if (op == Operation::READ) {
            op_ctr = items[item_id]->read_op_ctr;
        }
        else {
            op_ctr = items[item_id]->write_op_ctr;
            items[item_id]->read_op_ctr++;
        }
        items[item_id]->write_op_ctr++;
        return op_ctr;
    }

public:
    
    O2PL(ll m) {
        trans_id_ctr = 1;
        items.resize(m, nullptr);
        for (int i = 0; i < m; i++) {
            items[i] = new Item();
        }
        size = m;
    }

    ~O2PL() {
        for (int i = 0; i < size; i++) {
            delete items[i];
        }
    }

    Transaction* begin_trans() {
        ll id = trans_id_ctr.fetch_add(1);
        Transaction* t = new Transaction(id);
        return t;
    }

    void read(Transaction* t, ll item_id, ll& locVal) {
        ll op_ctr = get_op_ctr(item_id, Operation::READ);

        while (op_ctr > items[item_id]->write_item_ctr);

        locVal = items[item_id]->val;

        logEvent(t->id, item_id, Operation::READ);

        t->operations[item_id].push_back({op_ctr, Operation::READ});

        items[item_id]->read_item_ctr++;
    }

    void write(Transaction* t, ll item_id, ll newVal) {
        ll op_ctr = get_op_ctr(item_id, Operation::WRITE);

        while (op_ctr > items[item_id]->write_item_ctr + items[item_id]->read_item_ctr);

        items[item_id]->val = newVal;

        logEvent(t->id, item_id, Operation::WRITE);

        t->operations[item_id].push_back({op_ctr, Operation::WRITE});

        items[item_id]->write_item_ctr++;
    }

    void tryCommit(Transaction* t) {
        for (auto& [item_id, v] : t->operations) {
            if(v.size()==1) {
                ll ctr = v[0].first;
                Operation op = v[0].second;
                if (op == Operation::READ) {
                    while (ctr > items[item_id]->write_ulock_item_ctr);
                }
                else {
                    while (ctr > items[item_id]->write_ulock_item_ctr + items[item_id]->read_ulock_item_ctr);
                }
            }
            else {
                for(int i=0; i<v.size(); i++) {
                    ll ctr = v[i].first;
                    Operation op = v[i].second;
                    if (op == Operation::READ) {
                        while (ctr > items[item_id]->write_ulock_item_ctr + (v.back().second == Operation::READ));
                    }
                    else {
                        while (ctr > items[item_id]->write_ulock_item_ctr + items[item_id]->read_ulock_item_ctr + (v.back().second == Operation::WRITE));
                    }
                }
            }
        }

        logEvent(t->id, -1, Operation::COMMIT);

        for (auto& [item_id, v] : t->operations) {
            for(int i=0; i<v.size(); i++) {
                ll ctr = v[i].first;
                Operation op = v[i].second;
                if (op == Operation::READ) {
                    items[item_id]->read_ulock_item_ctr++;
                }
                else {
                    items[item_id]->write_ulock_item_ctr++;
                }
            }
        }
    }
};

O2PL* o2pl = nullptr;

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

    uniform_int_distribution<int> unifRand_idx(0, numItems - 1); // For random index
    uniform_int_distribution<int> unifRand_val(0, 100); // For random value
    bernoulli_distribution writeDist(writeProbab); // For write probability

    for (int i = 0; i < numTrans; i++) {
        Transaction* t = o2pl->begin_trans();

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
            o2pl->read(t, randInd, locVal);

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
                o2pl->write(t, randInd, locVal);

                // Update maxWriteScheduled
                maxWriteScheduled[randInd] = max(maxWriteScheduled[randInd], t->id);

                // Unlock the item
                item_locks[randInd]->unlock();
            }
        }

        // Try to commit the transaction
        o2pl->tryCommit(t);

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

    o2pl = new O2PL(numItems);

    item_locks.resize(numItems, new mutex());
    maxReadScheduled.resize(numItems, 0);
    maxWriteScheduled.resize(numItems, 0);
    num_item_accessed = 0;

    // Initialize the log file
    logFile.open("O2PL-log.txt");

    vector<thread> threads;

    ll startTime = getCurTime();

    for(int i=0; i < numThreads; i++) {
        threads.push_back(thread(work, i));
    }

    for(int i=0; i < numThreads; i++) {
        threads[i].join();
    }

    ll endTime = getCurTime();

    double avgCommitDelay = (double)(endTime - startTime) / (double)totalTrans;

    printf("Average time taken to commit a transaction: %.3lf microseconds\n", avgCommitDelay);

    double avg_item_accessed = (double)num_item_accessed / (double)totalTrans;
    printf("Average number of items accessed per transaction: %.3lf\n", avg_item_accessed);
    printf("Total number of items accessed: %lld\n", num_item_accessed.load());

    logFile.close();

    delete o2pl;

    return 0;
}