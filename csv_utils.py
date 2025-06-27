import csv
import utils
import json
import os
from datetime import datetime
import re

DIALECT = 'excel-tab'
COL_LIST = ['id', 'institutionCode', 'collectionCode', 'ownerInstitutionCode', 'basisOfRecord', 'occurrenceID',
    'catalogNumber', 'otherCatalogNumbers', 'higherClassification', 'kingdom', 'phylum', 'class', 'order', 'family',
    'scientificName', 'taxonID', 'scientificNameAuthorship', 'genus', 'subgenus', 'specificEpithet', 'verbatimTaxonRank',
    'infraspecificEpithet', 'taxonRank', 'identifiedBy', 'dateIdentified', 'identificationReferences', 'identificationRemarks', 
    'taxonRemarks', 'identificationQualifier', 'typeStatus', 'recordedBy', 'associatedCollectors', 'recordNumber', 'eventDate',
    'eventDate2', 'year', 'month', 'day', 'startDayOfYear', 'endDayOfYear', 'verbatimEventDate', 'occurrenceRemarks', 'habitat',
    'substrate', 'verbatimAttributes', 'fieldNumber', 'eventID', 'informationWithheld', 'dataGeneralizations', 'dynamicProperties',
    'associatedOccurrences', 'associatedSequences', 'associatedTaxa', 'reproductiveCondition', 'establishmentMeans', 'cultivationStatus',
    'lifeStage', 'sex', 'individualCount', 'preparations', 'locationID', 'continent', 'waterBody', 'islandGroup', 'island', 'country',
    'stateProvince', 'county', 'municipality', 'locality', 'locationRemarks', 'localitySecurity', 'localitySecurityReason', 'decimalLatitude',
    'decimalLongitude', 'geodeticDatum', 'coordinateUncertaintyInMeters', 'verbatimCoordinates', 'georeferencedBy', 'georeferenceProtocol',
    'georeferenceSources', 'georeferenceVerificationStatus', 'georeferenceRemarks', 'minimumElevationInMeters', 'maximumElevationInMeters',
    'minimumDepthInMeters', 'maximumDepthInMeters', 'verbatimDepth', 'verbatimElevation', 'disposition', 'language', 'recordEnteredBy',
    'modified', 'sourcePrimaryKey-dbpk', 'collID', 'recordID', 'references']

def clean_dict_val_whitespace(j):
    return {k: re.sub(r'\s+', ' ', v) if type(v) == str else v  for k,v in j.items()}

def clean_json_file_whitespace(in_json, out_json):
    with open(out_json, 'w') as out_f:
        with open(in_json, 'r') as in_f:
            for row in in_f:
                j = json.loads(row)
                j = clean_dict_val_whitespace(j)
                out_f.write(f'{json.dumps(j)}\n')

def convert_api_to_dl(j, target, **to_update):
    occ_id = str(j['occid'])
    # j['institutionCode'] = ## FROM COLL DATA
    j.update(**to_update)
    j['id'] = occ_id
    if j['sciname'] is not None:
        j['specificEpithet'] = j['sciname'].split(' ')[1] if ' ' in j['sciname'] else ''
        if j['taxonRank'] is not None:
            j['infraspecificEpithet'] = j['sciname'].split(f'{j["taxonRank"]} ')[-1]
        else:
            j['infraspecificEpithet'] = ''
    j['modified'] = j['dateLastModified']
    j['scientificName'] = j['sciname']
    j['taxonID'] = str(j['tidInterpreted'])
    j['sourcePrimaryKey-dbpk'] = j['dbpk']
    j['references'] = f'{utils.portal_root(target)}/collections/individual/index.php?occid={occ_id}'
    j['collID'] = str(j['collid'])
    return j

def add_json_to_raw_csv(in_json, out_csv):
    csv_exists = os.path.exists(out_csv)
    if csv_exists:
        # Get Keys from CSV
        with open(out_csv, 'r') as f:
            csv_reader = csv.reader(f, dialect=DIALECT)
            firstline = next(csv_reader)
        fieldnames = firstline
    else:
        # Get Keys from Json Lines
        with open(in_json, 'r') as f:
            firstline = json.loads(f.readline())
        fieldnames = firstline.keys()

    add_json_to_csv(in_json, out_csv, fieldnames)

def add_json_to_dl_csv(in_json, out_csv, target, **to_update):
    add_json_to_csv(in_json, out_csv, COL_LIST, json_convert=convert_api_to_dl, target=target, **to_update)

def add_json_to_csv(in_json, out_csv, fieldnames, json_convert=lambda x:x, **convert_args):
    csv_exists = os.path.exists(out_csv)
    default = {k:None for k in fieldnames}
    with open(out_csv, 'a', newline='', encoding='UTF-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, dialect=DIALECT)
        if not csv_exists:
            writer.writeheader()
        line_ct = utils.file_line_count(in_json)
        if line_ct > 0:
            with open(in_json, 'r') as f:
                for line in f:
                    j = json_convert({**default, **json.loads(line)}, **convert_args)
                    sub_j = {k:v for k,v in j.items() if k in fieldnames}
                    writer.writerow(sub_j)

def csv_line_count(fn):
    return utils.file_line_count(fn) - 1

# Count Occs per Coll in mega TSV
def occ_per_coll_in_csv(fn):
    coll_occ_cts = {}
    with open(fn, newline='') as csvfile:
        reader = csv.DictReader(csvfile, dialect=DIALECT)
        for row in reader:
            coll_id = row['collid']
            if coll_id not in coll_occ_cts:
                coll_occ_cts[coll_id] = 0
            coll_occ_cts[coll_id] +=1
    return coll_occ_cts

# Deduplicate JSON
def deduplicate_json(fn):
    occ_ids={}
    to_write=[]
    with open(fn,'r') as f:
        for row in f:
            # j = json.loads(row)
            # occ_id = str(j['occid'])

            _, occ_id, *_ = row.split(' ')
            occ_id = occ_id.replace(',', '')
            occ_id_k = occ_id[:2]
            occ_id_v = occ_id[2:]
            if occ_id_k not in occ_ids:
                occ_ids[occ_id_k] = []
            if occ_id_v not in occ_ids[occ_id_k]:
                to_write.append(row)
                occ_ids[occ_id_k].append(occ_id_v)
    with open(fn, 'w') as f:
        for r in to_write:
            f.write(r)

# Deduplicate CSV  (Unused)                  
# def deduplicate(fn, out):
#     occ_ids={}
#     with open(fn, newline='') as csvfile_in:
#         reader = csv.DictReader(csvfile_in, dialect=DIALECT)
#         with open(out, 'w', newline='') as csvfile_out:
#             writer = csv.DictWriter(csvfile_out, fieldnames=reader.fieldnames, dialect=DIALECT)
#             writer.writeheader()
#             for idx, row in enumerate(reader):
#                 occ_id = row['occid']
#                 coll_id = row['collid']
#                 if idx % 10000 == 0:
#                     print(idx)
#                 if coll_id not in occ_ids:
#                     occ_ids[coll_id] = {}
#                 occ_id_k = occ_id[:2]
#                 occ_id_v = occ_id[2:]
#                 if occ_id_k not in occ_ids[coll_id]:
#                     occ_ids[coll_id][occ_id_k] = []

#                 if occ_id_v not in occ_ids[coll_id][occ_id_k]:
#                     occ_ids[coll_id][occ_id_k].append(occ_id_v)
#                     writer.writerow(row)


## Deduplication Test
# in_tsv = './examples/duplicates.tsv'
# out_tsv='./examples/dedup.tsv'

# in_coll2_ct = occ_per_coll_in_csv(in_tsv)["2"]
# print(in_coll2_ct)
# print("Start Dedup1")
# start_d1 = datetime.now()
# deduplicate(in_tsv, out_tsv)
# end_d1 = datetime.now()
# out_coll2_ct = occ_per_coll_in_csv(out_tsv)["2"]
# print(out_coll2_ct)

# print("Start Dedup2")
# start_d2 = datetime.now()
# deduplicate2(in_tsv, out_tsv)
# end_d2 = datetime.now()
# out_coll2_ct = occ_per_coll_in_csv(out_tsv)["2"]
# print(out_coll2_ct)

# print(f"d1 time {end_d1-start_d1}")
# print(f"d2 time {end_d2-start_d2}")
## 298059(297753)
## Manual CSV creation Below

# full_tsv = './cnalh_full.tsv'
# run_no=1

# save_dir = f'./raw/run{run_no}/'
# raw_dir = f'{save_dir}raw/'
# json_files = sorted([f for f in os.listdir(raw_dir) if not f.startswith('occur')], key=lambda x:int(x.strip('.json')))

# print(json_files)
# start_csv = datetime.now()
# for fn in json_files:
#     full_fn = f"{raw_dir}{fn}"
#     add_json_to_master_csv(full_fn, full_tsv)

# end_csv = datetime.now()
# print(f"Time to create CSV: {end_csv - start_csv}")


## Manual Count Occurances per Coll
# start_ct = datetime.now()
# occ_ct = occ_per_coll_in_csv(full_tsv)
# end_ct = datetime.now()

# print(f"Time to count CSV: {end_ct - start_ct}")

## Manual Compare JSON to CSV Filesize
# full_json_fs = 0
# for fn in json_files:
#     full_fn = f"{raw_dir}{fn}"
#     full_json_fs += os.path.getsize(full_fn)

# csv_fs = os.path.getsize(full_tsv)
# print(f"JSON: {full_json_fs:,}, CSV: {csv_fs:,}")




