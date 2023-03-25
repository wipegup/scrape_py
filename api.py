import utils

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument('--total', action='store_true')
    parser.add_argument('--coll', type=int)

    args = parser.parse_args()

    if args.total:
        ct, *_ = utils.get_occ()
        print(f"Total Occurs = {ct}")

    if args.coll:
        _, coll_res, _ = utils.get_coll()
        col_rec = [r for r in coll_res if str(r['collID']) == str(args.coll)][0]

        ct, *_ = utils.get_occ(coll_id=args.coll)
        print(f"Collection {args.coll} -- Code: {col_rec['institutionCode']}; Name: {col_rec['collectionName']}; Occurs: {ct}")