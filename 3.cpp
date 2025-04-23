#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
#include <unordered_set>
#include <iomanip>
#include <ctime>
#include <atomic>
#include <thread>
#include <mutex>
#include <sstream>
#include <random>
#include <unistd.h>
using namespace std;
using namespace std::chrono;
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

class ReaderWriterLock {
private:
    set<ll> readers;
    ll writer_id;
    mutex rw_mtx;
public:
    ReaderWriterLock() : writer_id(-1) {}

    bool lock_read(ll transId) {
        rw_mtx.lock();

        // Edge case of same reader followed by writer
        if (writer_id == transId) {
            rw_mtx.unlock();
            return true;
        }

        if (writer_id != -1) {
            rw_mtx.unlock();
            return false;
        }

        readers.insert(transId);
        rw_mtx.unlock();
        return true;
    }

    void unlock_read(ll transId) {
        rw_mtx.lock();
        readers.erase(transId);
        rw_mtx.unlock();
    }

    bool lock_write(ll transId) {
        rw_mtx.lock();

        if ((readers.size() == 1) && (*readers.begin() == transId)) {
            readers.erase(transId);
            writer_id = transId;
            rw_mtx.unlock();
            return true;
        }

        if ((writer_id != -1) || (!readers.empty())) {
            rw_mtx.unlock();
            return false;
        }

        writer_id = transId;
        rw_mtx.unlock();
        return true;
    }

    void unlock_write(ll transId) {
        rw_mtx.lock();
        writer_id = -1;
        rw_mtx.unlock();
    }
};

class Item {
public:
    ReaderWriterLock rw_lock;
    ll val;

    Item() {
        val = 0;
    }
};

class Transaction {
public:
    ll id;
    set<ll> read_set, write_set;

    Transaction(ll id) {
        this->id = id;
    }
};

class SS2PL {
private:
    vector<Item*> items;
    ll size;
    atomic<ll> trans_id_ctr;

public:
    SS2PL(ll m) {
        items.resize(m, nullptr);
        for (int i = 0; i < m; i++) {
            items[i] = new Item();
        }
        size = m;
    }

    ~SS2PL() {
        for (int i = 0; i < size; i++) {
            delete items[i];
        }
    }

    Transaction* begin_trans() {
        ll id = trans_id_ctr.fetch_add(1);
        Transaction* t = new Transaction(id);
        return t;
    }

    bool try_read(Transaction* trans, ll item_id, ll& locVal) {
        bool succ = items[item_id]->rw_lock.lock_read(trans->id);

        if(!succ) {
            return false;
        }
        locVal = items[item_id]->val;

        logEvent(trans->id, item_id, Operation::READ);

        trans->read_set.insert(item_id);

        return true;
    }

    bool try_write(Transaction* trans, ll item_id, ll newVal) {
        bool succ = items[item_id]->rw_lock.lock_write(trans->id);

        if(!succ) {
            return false;
        }
        items[item_id]->val = newVal;

        logEvent(trans->id, item_id, Operation::WRITE);

        trans->write_set.insert(item_id);

        return true;
    }

    void try_commit(Transaction* trans) {
        for (auto& item: trans->read_set) {
            items[item]->rw_lock.unlock_read(trans->id);
        }

        for (auto& item: trans->write_set) {
            items[item]->rw_lock.unlock_write(trans->id);
        }
    }
};

SS2PL* ss2pl = nullptr;

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
        Transaction* t = ss2pl->begin_trans();

        // Choose numIters random items to be updated
        unordered_set<ll> randIndices;
        while (randIndices.size() < numIters) {
            ll randInd = unifRand_idx(random_number_generator);
            randIndices.insert(randInd);
        }

        for (auto& randInd : randIndices) {
            ll locVal;

            bool flag = true;

            while (true) {
                item_locks[randInd]->lock();

                if (!canRead(randInd, t->id)) {
                    flag = false;
                    item_locks[randInd]->unlock();
                    break;
                }

                bool succ = ss2pl->try_read(t, randInd, locVal);

                if (succ) {
                    num_item_accessed++;
                    // Update maxReadScheduled
                    maxReadScheduled[randInd] = max(maxReadScheduled[randInd], t->id);
                    item_locks[randInd]->unlock();
                    break;
                }
                else {
                    item_locks[randInd]->unlock();
                }
            }

            if (!flag) continue;
            
            bool write = writeDist(random_number_generator);

            if (write) {

                // Update the local value
                locVal += unifRand_val(random_number_generator);

                while (true) {
                    item_locks[randInd]->lock();

                    if (!canWrite(randInd, t->id)) {
                        item_locks[randInd]->unlock();
                        break;
                    }

                    bool succ = ss2pl->try_write(t, randInd, locVal);

                    if (succ) {
                        // Update maxWriteScheduled
                        maxWriteScheduled[randInd] = max(maxWriteScheduled[randInd], t->id);
                        item_locks[randInd]->unlock();
                        break;
                    }
                    else {
                        item_locks[randInd]->unlock();
                    }
                }
            }
        }

        // Try to commit the transaction
        ss2pl->try_commit(t);
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

    ss2pl = new SS2PL(numItems);

    item_locks.resize(numItems, new mutex());
    maxReadScheduled.resize(numItems, 0);
    maxWriteScheduled.resize(numItems, 0);
    num_item_accessed = 0;

    // Initialize the log file
    logFile.open("SS2PL-log.txt");

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

    delete ss2pl;

    return 0;
}