#include <bits/stdc++.h>
#include <unistd.h>
using namespace std;
using namespace chrono;
typedef long long ll;

enum class Operation {
    READ,
    WRITE
};

class Transaction {
public:
    ll id;
    // // Map of item id to pair of operatin id and operation type
    map<ll, vector<pair<ll, Operation>>> operations;
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

    void read(Transaction* t, ll item_id, ll& loc) {
        ll op_ctr = get_op_ctr(item_id, Operation::READ);

        while (op_ctr > items[item_id]->write_item_ctr);

        loc = items[item_id]->val;

        t->operations[item_id].push_back({op_ctr, Operation::READ});

        items[item_id]->read_item_ctr++;
    }

    void write(Transaction* t, ll item_id, ll newVal) {
        ll op_ctr = get_op_ctr(item_id, Operation::WRITE);

        while (op_ctr > items[item_id]->write_item_ctr + items[item_id]->read_item_ctr);

        items[item_id]->val = newVal;

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
        cout << "Transaction " << t->id << " committed" << endl;
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

ll n, m;

vector<tuple<ll,char,ll>> ops;
vector<bool> done;

void work(ll tid) {
    ll i = 0;
    Transaction* t = new Transaction();
    t->id = tid;
    while(i<done.size()) {
        auto [tid1, op, iid] = ops[i];
        if(tid1 == tid) {
            if(op=='c') {
                done[i] = true;
                o2pl->tryCommit(t);
            }
            else {
                ll loc = (i*tid)^i;
                if(op=='r') {
                    o2pl->read(t, iid, loc);
                }
                else {
                    o2pl->write(t, iid, loc);
                }
                cout << "Thread " << tid << " done with operation " << op << " " << iid << endl;
                done[i] = true;
            }
        }
        while(!done[i]);
        i++;
    }

    delete t;
}

int main(int argc, char* argv[]) {
    ll lines;

    string inp = argv[1];

    FILE* f = fopen(inp.c_str(), "r");
    fscanf(f, "%lld %lld %lld", &n, &m, &lines);

    o2pl = new O2PL(m);

    for(int i=0; i<lines; i++) {
        ll tid, iid;
        char op;
        fscanf(f, "%lld %c", &tid, &op);

        if(op=='c') {
            ops.push_back({tid, op, -1});
        }
        else {
            fscanf(f, "%lld", &iid);
            ops.push_back({tid, op, iid});
        }
        done.push_back(false);
    }

    vector<thread> threads;

    for(int i=0; i<n; i++) {
        threads.push_back(thread(work, i));
    }

    for(int i=0; i<n; i++) {
        threads[i].join();
    }

    delete o2pl;

    return 0;
}