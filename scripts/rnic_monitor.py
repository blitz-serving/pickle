# ref: https://docs.nvidia.com/networking/display/ufmenterpriseumv6150/appendix+-+supported+port+counters+and+events

import subprocess
import time


class Monitor:
    def __init__(self, rnic, events: dict[str, list]):
        self.rnic = rnic
        self.pre_event_data = {}
        self.events = events
        for tag, tuple in events.items():
            self.pre_event_data[tag] = tuple[1](
                Monitor.get_rnic_counter(rnic, tuple[0])
            )

    def report(self, elapsed):
        printed = False
        for tag, tuple in self.events.items():
            new_value = tuple[1](Monitor.get_rnic_counter(self.rnic, tuple[0]))
            delta = new_value - self.pre_event_data[tag]
            self.pre_event_data[tag] = new_value
            if delta > 1:
                if not printed:
                    print(f"{self.rnic}:", end="\t")
                printed = True
                print(f"{tag}: {round(delta / elapsed, 2)}", end="\t")
        if printed:
            print()

    @staticmethod
    def get_rnic_counter(rnic, event):
        return int(
            subprocess.check_output(
                ["cat", f"/sys/class/infiniband/{rnic}/ports/1/counters/{event}"]
            )
        )


def main():
    rnics = ["mlx5_0", "mlx5_1"]
    events = {
        "recv_pkts": ["port_rcv_packets", lambda x: x / 1e6],
        "recv_data": ["port_rcv_data", lambda x: 4.0 * x / 1e9],
        "xmit_pkts": ["port_xmit_packets", lambda x: x / 1e6],
        "xmit_data": ["port_xmit_data", lambda x: 4.0 * x / 1e9],
    }
    start = time.time()
    monitors = [Monitor(rnic, events) for rnic in rnics]
    while True:
        end = time.time()
        elapsed = end - start
        start = end

        for m in monitors:
            m.report(elapsed)
        time.sleep(1)
        print("-" * 80)


if __name__ == "__main__":
    main()
