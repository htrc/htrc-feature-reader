import argparse
from htrc_features.utils import id_to_rsync


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("id", type=str)
    parser.add_argument("--advanced", action="store_true")
    args = parser.parse_args()
    print(id_to_rsync(args.id, args.advanced if args.advanced else 'basic'))

if __name__ == '__main__':
    main()
