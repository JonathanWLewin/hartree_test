# %%
import apache_beam as beam
import typing

# %%
HEADER_ROW='legal_entity,counter_party,tier,max(rating by counterparty),sum(value where status = ARAP),sum(value where status = ACCR)'

# %%
class Dataset(typing.NamedTuple):
    invoice_id: int
    legal_entity: str
    counter_party: str
    rating: int
    status: str
    value: float
    tier: int

class OutDataset(typing.NamedTuple):
    legal_entity: str
    counter_party: str
    tier: int
    max_rating_by_cp: int
    sum_value_arap: float
    sum_value_accr: float

# %%
class SplitCsvContent(beam.DoFn):
    def process(self, element):
        yield tuple(element.split(','))

# %%
class CreateKVPairOfContentByIndexList(beam.DoFn):
    def process(self, element, index_list):
        yield (([element[x] for x in index_list], element))

# %%
deconstruct_left_join = lambda x: [y + (x[1]['right'][0][-1] if x[1]['right'] != [] else 0.0,) for y in x[1]['left']]
deconstruct_right_join = lambda x: [y[:-1] + (x[1]['left'][0][-1] if x[1]['left'] != [] else 0.0,) + (y[-1],) for y in x[1]['right']]
deconstruct_outer_join = lambda x: deconstruct_left_join(x) + deconstruct_right_join(x)

# %%
class DeconstructJoin(beam.PTransform):
    def expand(self, pcoll):
        return (pcoll | beam.FlatMap(deconstruct_outer_join) \
                | beam.Distinct())

# %%
class GroupByCalculateSumValueAndCreateIndexByKV(beam.PTransform):
    def __init__(self, column_list):
        self.column_list = column_list
    def expand(self, pcoll):
        return (pcoll | beam.GroupBy(self.column_list).aggregate_field('value', sum, 'value_sum') \
                    | beam.ParDo(CreateKVPairOfContentByIndexList(), range(len(self.column_list))))

# %%
p = beam.Pipeline()

# %%
#Resolve differences with \r and \n
text1: str
text2: str

with open ('dataset1.csv', 'r') as d1:
    text1 = d1.read()
    text1.replace('\r', '\n')
with open('dataset1.csv', 'w') as d1:
    d1.write(text1)

with open ('dataset2.csv', 'r') as d2:
    text2 = d2.read()
    text2.replace('\r', '\n')
with open('dataset2.csv', 'w') as d2:
    d2.write(text2)

# %%
#Bring in data
dataset1 = p | "Read1" >> beam.io.ReadFromText('dataset1.csv', skip_header_lines=1)
dataset2 = p | "Read2" >> beam.io.ReadFromText('dataset2.csv', skip_header_lines=1)

# %%
#Split csv content out
data_split = (dataset1 | "SplitCsv1" >> beam.ParDo(SplitCsvContent()) \
            | "ConvertToKVPair1" >> beam.ParDo(CreateKVPairOfContentByIndexList(), [2]))
data_tier_split = (dataset2 | "SplitCsv2" >> beam.ParDo(SplitCsvContent()) \
            | "ConvertToKVPair2" >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0]))

# %%
#Get Tier
combined_data_struct = ({'left': data_split, 'right': data_tier_split} | beam.CoGroupByKey() \
                        | beam.FlatMap(deconstruct_left_join))

# %%
#Convert to schema
data_schema = combined_data_struct | beam.Map(lambda x: Dataset(
    invoice_id=int(x[0]),
    legal_entity=x[1],
    counter_party=x[2],
    rating=int(x[3]),
    status=x[4],
    value=float(x[5]),
    tier=int(x[6]))).with_output_types(Dataset)

# %%
#Get max rating by cp
max_rating_by_cp = data_schema | "MaxRatingByCP" >> beam.GroupBy('counter_party').aggregate_field('rating', max, 'max_rating')

# %%
#Split into ARAP and ACCR
arap_dataset = data_schema | 'SplitArap' >> beam.Filter(lambda x: x[4] == 'ARAP')
accr_dataset = data_schema | 'SplitAccr' >> beam.Filter(lambda x: x[4] == 'ACCR')

# %%
#Retrieve LE Totals
le_group_by_arap = (arap_dataset | "GroupByLeArap" >> beam.GroupBy('legal_entity').aggregate_field('value', sum, 'value_sum_arap') \
                    | 'CreateKVPairLeArap' >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0]))
le_group_by_accr = (accr_dataset | "GroupByLeAccr" >> beam.GroupBy('legal_entity').aggregate_field('value', sum, 'value_sum_accr') \
                    | 'CreateKVPairLeAccr' >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0]))
le_group_by = {'left': le_group_by_arap, 'right': le_group_by_accr} | "MergeLe" >> beam.CoGroupByKey()
le_group_by = le_group_by | "DeconstructLe" >> DeconstructJoin() \
        |beam.Map(lambda x: OutDataset(
    legal_entity=x[0],
    counter_party='Total',
    tier='Total',
    max_rating_by_cp='N/A',
    sum_value_arap=x[1],
    sum_value_accr=x[2]
)).with_output_types(OutDataset)

# %%
#Retrieve LE, CP Totals 
le_cp_group_by_arap = (arap_dataset | "GroupByLeCpArap" >> beam.GroupBy("legal_entity", "counter_party").aggregate_field('value', sum, 'value_sum_arap') \
                       | 'CreateKVPairLeCpArap' >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0, 1]))
le_cp_group_by_accr = (accr_dataset | "GroupByLeCpAccr" >> beam.GroupBy("legal_entity", "counter_party").aggregate_field('value', sum, 'value_sum_accr') \
                       | 'CreateKVPairLeCpAccr' >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0, 1]))
le_cp_group_by = {'left': le_cp_group_by_arap, 'right': le_cp_group_by_accr} | "MergeLeCp" >> beam.CoGroupByKey()
le_cp_group_by = (le_cp_group_by | "DeconstructLeCp" >> DeconstructJoin() \
                  | beam.Map(lambda x: OutDataset(
    legal_entity=x[0],
    counter_party=x[1],
    tier='Total',
    max_rating_by_cp='N/A',
    sum_value_arap=x[2],
    sum_value_accr=x[3]
)).with_output_types(OutDataset))

# %%
#Retrieve CP Rows
cp_group_by_arap = (arap_dataset | "GroupByCpArap" >> beam.GroupBy('counter_party').aggregate_field('value', sum, 'value_sum_arap') \
                    | 'CreateKVPairCpArap' >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0]))
cp_group_by_accr = (accr_dataset | "GroupByCpAccr" >> beam.GroupBy('counter_party').aggregate_field('value', sum, 'value_sum_accr') \
                    | 'CreateKVPairCpAccr' >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0]))
cp_group_by = {'left': cp_group_by_arap, 'right': cp_group_by_accr} | "MergeCp" >> beam.CoGroupByKey()
cp_group_by = (cp_group_by | "DeconstructCp" >> DeconstructJoin() \
               | beam.Map(lambda x: OutDataset(
    legal_entity='Total',
    counter_party=x[0],
    tier='Total',
    max_rating_by_cp='N/A',
    sum_value_arap=x[1],
    sum_value_accr=x[2]
)).with_output_types(OutDataset))

# %%
#Retrieve TI Rows
ti_group_by_arap = (arap_dataset | "GroupByTiArap" >> beam.GroupBy('tier').aggregate_field('value', sum, 'value_sum_arap') \
                    | 'CreateKVPairTiArap' >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0]))
ti_group_by_accr = (accr_dataset | "GroupByTiAccr" >> beam.GroupBy('tier').aggregate_field('value', sum, 'value_sum_accr') \
                    | 'CreateKVPairTiAccr' >> beam.ParDo(CreateKVPairOfContentByIndexList(), [0]))
ti_group_by = {'left': ti_group_by_arap, 'right': ti_group_by_accr} | "MergeTi" >> beam.CoGroupByKey()
ti_group_by = (ti_group_by | "DeconstructTi" >> DeconstructJoin() \
               | beam.Map(lambda x: OutDataset(
    legal_entity='Total',
    counter_party='Total',
    tier=x[0],
    max_rating_by_cp='N/A',
    sum_value_arap=x[1],
    sum_value_accr=x[2]
)).with_output_types(OutDataset))

# %%
#Consolidate data
consolidated_data = (le_group_by, le_cp_group_by, cp_group_by, ti_group_by) | beam.Flatten()

# %%
#Group by CP to get max rating by CP
consolidated_data_group_by_cp = consolidated_data | beam.GroupBy('counter_party')
out_data = ({'left': consolidated_data_group_by_cp, 'right': max_rating_by_cp} | "GetMaxRating" >> beam.CoGroupByKey() \
            | beam.FlatMap(lambda x: [tuple(y) + (x[1]['right'][0] if x[1]['right'] != [] else 'N/A',) for y in x[1]['left'][0]]) \
            | beam.Map(lambda x: '%s,%s,%s,%s,%s,%s' % (x[0],x[1],x[2],x[6],x[4],x[5])))

# %%
#Output to file
out_data | beam.io.WriteToText('out_beam', '.csv', header=HEADER_ROW)
p.run().wait_until_finish()


