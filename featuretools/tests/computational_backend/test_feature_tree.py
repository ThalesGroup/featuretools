# -*- coding: utf-8 -*-

import pytest

from featuretools.tests.testing_utils.mock_ds import make_nip_entityset
from featuretools.synthesis.deep_feature_synthesis import DeepFeatureSynthesis
from featuretools.computational_backends.feature_tree import FeatureTree
from collections import defaultdict

@pytest.fixture(scope='module')
def entityset():
    return make_nip_entityset()

def _pretty(d, indent=0):
    strr = ""
    for key, value in d.items():
        strr += ('\t' * indent + str(key))
        if isinstance(value, dict):
            strr += _pretty(value, indent + 1)
        else:
            strr += ('\t' * (indent + 1) + str(value))
    return strr


def test_nip_feature_dag_2_depth(entityset):
    ignore_variables = defaultdict(set)
    ignore_variables["incidents"] = []
    ignore_variables["incidents"].append("ticketnum")
    ignore_variables["incidents"].append("incident")
    ignore_variables["incidents"].append("incident")

    dfs_object = DeepFeatureSynthesis("incidents", entityset,
                                      agg_primitives=["sum","variance"],
                                      trans_primitives=["dayhour","previousrowtimediff"],
                                      max_depth=2,
                                      ignore_variables=ignore_variables)
    features, all_features = dfs_object.build_features(verbose=False)
    feature_tree = FeatureTree(entityset, features=features, maxdepth=2)
    resultt = "0	incidents->incidentsserial		['Group_By', 'incidents.serial::incidentsserial.serial', 'serverserial,VARIANCE(incidents.serverserial),VARIANCE', 'record_ts,SUM(incidents.record_ts),SUM', 'numsymptoms,VARIANCE(incidents.numsymptoms),VARIANCE', 'serverserial,SUM(incidents.serverserial),SUM', 'record_ts,VARIANCE(incidents.record_ts),VARIANCE', 'numsymptoms,SUM(incidents.numsymptoms),SUM']	incidents		['Transformation', 'lastmodified,DAYHOUR(lastmodified),DayHour', 'ticketcreatetime,DAYHOUR(ticketcreatetime),DayHour', 'resolvedtime,DAYHOUR(resolvedtime),DayHour', 'deletedtime,DAYHOUR(deletedtime),DayHour', 'ticketcreatetime,PREVIOUSROWTIMEDIFF(ticketcreatetime),PreviousRowTimeDiff', 'lastoccurrence,PREVIOUSROWTIMEDIFF(lastoccurrence),PreviousRowTimeDiff', 'lastmodified,PREVIOUSROWTIMEDIFF(lastmodified),PreviousRowTimeDiff', 'lastoccurrence,DAYHOUR(lastoccurrence),DayHour', 'nocviewtime,DAYHOUR(nocviewtime),DayHour', 'deletedat,PREVIOUSROWTIMEDIFF(deletedat),PreviousRowTimeDiff', 'acknowledgedtime,DAYHOUR(acknowledgedtime),DayHour', 'deletedtime,PREVIOUSROWTIMEDIFF(deletedtime),PreviousRowTimeDiff', 'firstoccurrence,PREVIOUSROWTIMEDIFF(firstoccurrence),PreviousRowTimeDiff', 'nocviewtime,PREVIOUSROWTIMEDIFF(nocviewtime),PreviousRowTimeDiff', 'tacviewtime,DAYHOUR(tacviewtime),DayHour', 'acknowledgedtime,PREVIOUSROWTIMEDIFF(acknowledgedtime),PreviousRowTimeDiff', 'firstoccurrence,DAYHOUR(firstoccurrence),DayHour', 'tacviewtime,PREVIOUSROWTIMEDIFF(tacviewtime),PreviousRowTimeDiff', 'deletedat,DAYHOUR(deletedat),DayHour', 'resolvedtime,PREVIOUSROWTIMEDIFF(resolvedtime),PreviousRowTimeDiff']	incidentsserial->incidents		['Transformation', 'incidentsserial.serial::incidents.serial', 'rowzerocount_serial,incidentsserial.rowzerocount_serial', 'rownullcount_serial,incidentsserial.rownullcount_serial']1	incidentsserial->incidents		['Transformation', 'incidentsserial.serial::incidents.serial', 'VARIANCE(incidents.serverserial),incidentsserial.VARIANCE(incidents.serverserial)', 'SUM(incidents.record_ts),incidentsserial.SUM(incidents.record_ts)', 'VARIANCE(incidents.numsymptoms),incidentsserial.VARIANCE(incidents.numsymptoms)', 'SUM(incidents.serverserial),incidentsserial.SUM(incidents.serverserial)', 'VARIANCE(incidents.record_ts),incidentsserial.VARIANCE(incidents.record_ts)', 'SUM(incidents.numsymptoms),incidentsserial.SUM(incidents.numsymptoms)']"
    assert (resultt == _pretty(feature_tree.table_feature_operations_mapping_dag))


def test_nip_feature_dag_0_depth(entityset):
    ignore_variables = defaultdict(set)
    ignore_variables["incidents"] = []
    ignore_variables["incidents"].append("ticketnum")
    ignore_variables["incidents"].append("incident")
    ignore_variables["incidents"].append("incident")

    dfs_object = DeepFeatureSynthesis("incidents", entityset,
                                      agg_primitives=["count", "sum", "variance", "avg", "min", "max", "nuniq",
                                                      "stddev", "last", "first"],
                                      trans_primitives=["day", "month", "year", "weekday", "monthday", "dayhour",
                                                        "previousrowtimediff"],
                                      max_depth=0,
                                      ignore_variables=ignore_variables)
    features, all_features = dfs_object.build_features(verbose=False)
    feature_tree = FeatureTree(entityset, features=features, maxdepth=0)
    assert ("" == _pretty(feature_tree.table_feature_operations_mapping_dag))