from argparse import ArgumentParser
import sys


def main(argv=sys.argv):
    cmd = ArgumentParser(description="Import sas7bdat files to sqlite3 dbase")

    # TODO define your CLI here

    args = cmd.parse_args(argv)

    # TODO execute with parsed arguments

    print(args)


if __name__ == "__main__":
    main()
