from __future__ import print_function

import sys


def filter_line(file, substring):
    with open(file, "r") as fh:
        lines = fh.readlines()

    lines = [l for l in lines if not substring in l]

    with open(file, "w") as fh:
        fh.writelines(lines)


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        filter_line(*args)
    else:
        print("usage: {} <file> <substring>".format(sys.argv[0]))
