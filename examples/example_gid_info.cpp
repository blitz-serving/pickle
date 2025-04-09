#include <infiniband/verbs.h>

#include <iostream>

using namespace std;

int main() {
    ibv_device** dev_list = ibv_get_device_list(nullptr);
    for (int i = 0; dev_list[i] != nullptr; i++) {
        const char* dev_name = ibv_get_device_name(dev_list[i]);
        cout << "Device " << i << ": " << dev_name << endl;
        ibv_context* context = ibv_open_device(dev_list[i]);
        ibv_gid_entry entries[16];
        int num_entries = _ibv_query_gid_table(context, entries, 16, 0, sizeof(entries[0]));
        for (int j = 0; j < num_entries; j++) {
            cout << "gid index: " << entries[j].gid_index << " gid type: " << entries[j].gid_type << " gid: ";
            for (int k = 0; k < 16; k++) {
                printf("%02x:", (entries[j].gid.raw[k]));
            }
            cout << endl;
        }
        ibv_close_device(context);
    }
    ibv_free_device_list(dev_list);
}