import utils

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--total', action='store_true')
    parser.add_argument('--coll', type=int)
    parser.add_argument('--all-coll', action='store_true')
    parser.add_argument('--coll-count', action='store_true')

    args = parser.parse_args()

    if args.total:
        ct, *_ = utils.get_occ()
        print(f"Total Occurs = {ct:,}")

    if args.coll or args.all_coll or args.coll_count:
        coll_ct, coll_res, _ = utils.get_coll()

    if args.coll:
        col_rec = [r for r in coll_res if str(r['collID']) == str(args.coll)][0]

        ct, *_ = utils.get_occ(coll_id=args.coll)
        print(f"Collection {args.coll} -- Code: {col_rec['institutionCode']}; Name: {col_rec['collectionName']}; Occurs: {ct:,}")

    if args.all_coll:
        to_print = [(r['collID'], r['institutionCode'], r['collectionName']) for r in coll_res]
        for e in to_print:
            print(e)

    if args.coll_count:
        print(f'Currently there are {coll_ct} collections')
