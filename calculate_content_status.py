# -*- coding=utf-8 -*-
import sys
import os
import math, json
import random
import time
import re
import requests as REQ
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search, Q
import argparse as ap
import logging


from TimeUtils import *
from RedisManager import RedisManager
from EsManager import EsManager
from MyLogger import Logger
from hbase_client import HappyBaseClient
from kafka_client import Kafka_consumer, Kafka_producer
from persistent_module_image import PersistentModule


class CalculateContentStatus:
    def __init__(self, param_spec):
        self.param_spec = param_spec
        self.hbase = HappyBaseClient()
        self.es = EsManager().es
        self.prod1 = RedisManager("video_prod1")
        self.persist_util = PersistentModule(param_spec)
        self.log = Logger(
            self.param_spec["target_dir"] + "/log/calculate_content_status.txt",
            level=logging.INFO,
        )

    def GetOnePendantStatus(self, mgdb_obj):
        status = 1
        try:
            if mgdb_obj["activityType"] in [0, 1]:
                """查询外部数据源-赛事赛季上线状态判断"""
                if mgdb_obj.get("projectId", None):
                    competition_info = self.persist_util.GetProgramFromES(
                        "video_pendant_competition_status", mgdb_obj["projectId"]
                    )
                    if competition_info is not None:
                        if competition_info.get("competition_status", "1") != "1":
                            print("挂件赛事下线:", mgdb_obj["mgdbId"])
                            status = 0
                if mgdb_obj.get("seasonId", None):
                    season_info = self.persist_util.GetProgramFromES(
                        "video_pendant_season_status", mgdb_obj["seasonId"]
                    )
                    if season_info is not None:
                        if season_info.get("season_status", "1") != "1":
                            print("挂件赛季下线:", mgdb_obj["mgdbId"])
                            status = 0
                if mgdb_obj.get("testMgdb", "0") == "1":
                    print("测试挂件:", mgdb_obj["mgdbId"])
                    status = 0
                if "测试" in str(mgdb_obj.get("title", "")):
                    print("测试挂件(title=测试):", mgdb_obj["mgdbId"])
                    status = 0
                if mgdb_obj.get("mgdbStatus", 0) == 0:
                    print("下线挂件:", mgdb_obj["mgdbId"])
                    status = 0
            elif mgdb_obj["activityType"] in [4]:
                if mgdb_obj.get("test", None):
                    print("测试二台(test字段):", mgdb_obj["mgdbId"])
                    status = 0
                if mgdb_obj.get("roomLabel", {}).get("firstClassName", None) == "测试":
                    print("测试二台(cate_first=测试):", mgdb_obj["mgdbId"])
                    status = 0
                if "测试" in str(
                    mgdb_obj.get("shareMaterial", {}).get("roomTitle", "")
                ):
                    print("测试二台(title=测试):", mgdb_obj["mgdbId"])
                    status = 0
                if mgdb_obj["roomStatus"] != "3" or mgdb_obj["broadcastStatus"] != "1":
                    print("直播间下线:", mgdb_obj["mgdbId"])
                    status = 0
        except Exception as e:
            print(e)
            return None

        return status

    def ConsumerPendantKafka(
        self,
    ):
        """
        消费挂件消息
        """
        bootstrap_servers = self.param_spec["kafka_pendant"]["bootstrap_servers"]
        topic_rec = self.param_spec["kafka_pendant"]["topic_rec"]
        group_rec = "consume_pendant_group"
        #        self.consumer = Kafka_consumer(bootstrap_servers, topic_rec, group_rec, auto_offset_reset='earliest')
        self.consumer = Kafka_consumer(
            bootstrap_servers, topic_rec, group_rec, auto_offset_reset="latest"
        )

        for msg in self.consumer.consume_data():
            try:
                #                self.log.logger.info("kafka消息:"+str(msg))
                obj = json.loads(msg.value.decode("utf8"))
                if obj["channel"] not in ["noms-worldcup"]:
                    print(
                        "非小屏挂件过滤:",
                        obj["activityType"],
                        obj["channel"],
                        obj["mgdbId"],
                    )
                    continue

                data = json.loads(obj["data"])["body"]
                if obj["activityType"] in [0, 1]:
                    data["mgdbStatus"] = obj["mgdbStatus"]
                    rkey = "pendant_version:" + data["mgdbId"]
                    rvalue = self.prod1.Get(rkey)
                    if rvalue:
                        if int(obj["version"]) < int(rvalue):
                            continue
                    status = self.GetOnePendantStatus(data)
                    self.prod1.Set(rkey, int(obj["version"]), True, 7 * 24 * 60 * 60)

                    if not status:
                        print(
                            "挂件下线:",
                            data["mgdbId"],
                            obj["activityType"],
                            obj["channel"],
                            obj["mgdbId"],
                            obj["mgdbStatus"],
                            obj["version"],
                            obj["time"],
                        )
                        """更新hbase-all"""
                        program_hbase = self.persist_util.GetObjectFromHbase(
                            self.param_spec["hbase_all"],
                            ["cf:doc_type"],
                            [data["mgdbId"]],
                        )
                        if (
                            program_hbase
                            and program_hbase[data["mgdbId"]]
                            .get(b"cf:doc_type", b"")
                            .decode()
                            == "pendant"
                        ):
                            item = {
                                "doc_id": data["mgdbId"],
                                "status": 0,
                                "additional_status": 0,
                                "update_time": int(time.time()),
                            }
                            self.persist_util.SendToKafka([item])
                            self.persist_util.SendToHbase(
                                self.param_spec["hbase_all"], [item]
                            )
                            print(
                                "挂件下线状态更新HBASE成功!",
                                data["mgdbId"],
                                program_hbase,
                                item,
                            )
                            self.log.logger.info(
                                "kafka下线挂件消息:"
                                + str(program_hbase)
                                + "\t"
                                + str(item)
                            )
                            self.prod1.Set(
                                "video_kafka_pendant_poff",
                                "monitor",
                                True,
                                12 * 60 * 60,
                            )

                        """更新hbase-latest"""
                        try:
                            program_hbase = self.persist_util.GetObjectFromHbase(
                                self.param_spec["hbase_pendant_latest"],
                                ["cf:doc_type"],
                                [data["mgdbId"]],
                            )
                            if (
                                program_hbase
                                and program_hbase[data["mgdbId"]]
                                .get(b"cf:doc_type", b"")
                                .decode()
                                == "pendant"
                            ):
                                item = {
                                    "doc_id": data["mgdbId"],
                                    "status": 0,
                                    "additional_status": 0,
                                }
                                self.persist_util.SendToHbase(
                                    self.param_spec["hbase_pendant_latest"], [item]
                                )
                                print(
                                    "挂件下线状态更新HBASE成功!",
                                    data["mgdbId"],
                                    program_hbase,
                                    item,
                                )
                        except Exception as e:
                            print(e)

                        """更新内容画像hbase"""
                        try:
                            item = {
                                "doc_id": data["mgdbId"],
                                "status": 0,
                                "additional_status": 0,
                            }
                            self.persist_util.SendToHbase(
                                self.param_spec["hbase_cp"], [item]
                            )
                            print("挂件下线状态更新HBASE-CP成功!", data["mgdbId"], item)
                        except Exception as e:
                            print(e)

                        """更新搜推ES"""
                        try:
                            program = self.persist_util.GetProgramFromES(
                                self.param_spec["es_byte_idx"], data["mgdbId"]
                            )
                            if program and program.get("doc_type", "") == "pendant":
                                res = self.es.update(
                                    index=self.param_spec["es_byte_idx"],
                                    id=data["mgdbId"],
                                    body={"doc": {"status": 0, "additional_status": 0}},
                                )
                                print(
                                    "挂件下线状态更新搜推ES成功!",
                                    data["mgdbId"],
                                    program,
                                    res,
                                )
                        except Exception as e:
                            print(e)

                        #                        """更新redis by http"""
                        #                        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
                        #                        post_data = {}
                        #                        post_data["operatingUser"] = "zhangcong_yy"
                        #                        post_data["dataType"] = "zset"
                        #                        post_data["operateType"] = "write"
                        #                        post_data["addData"] = {"key":"video_search_recomm_content_offline", "valueZSet":{data['mgdbId']:int(obj['version'])}}
                        #                        print (json.dumps(post_data, ensure_ascii=False))
                        #                        readed = REQ.post(url=url, json=post_data)
                        #                        print ("挂件下线状态更新HTTP成功!", readed.json())

                        """更新redis by http"""
                        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
                        post_data = {}
                        post_data["operatingUser"] = "zhangcong_yy"
                        post_data["dataType"] = "hash"
                        post_data["operateType"] = "query"
                        post_data["args"] = "exists video_cp:" + data["mgdbId"]
                        readed = REQ.post(url=url, json=post_data)
                        print("query:", readed.json(), type(readed.json()))
                        if readed.json()["data"] == 1:
                            post_data = {}
                            post_data["operatingUser"] = "zhangcong_yy"
                            post_data["operateType"] = "del"
                            post_data["delData"] = {"key": "video_cp:" + data["mgdbId"]}
                            readed = REQ.post(url=url, json=post_data)
                            print("下线redis key删除成功!", readed.json())

                elif obj["activityType"] in [4]:
                    print(
                        "二台直播间:",
                        data["mgdbId"],
                        obj["activityType"],
                        obj["channel"],
                        obj["mgdbId"],
                        obj["version"],
                        obj["time"],
                    )
                    rkey = "pendant_version:" + data["mgdbId"][4:]
                    rvalue = self.prod1.Get(rkey)
                    if rvalue:
                        if int(obj["version"]) < int(rvalue):
                            continue
                    status = self.GetOnePendantStatus(data)
                    self.prod1.Set(rkey, int(obj["version"]), True, 7 * 24 * 60 * 60)

                    if not status:
                        """更新hbase"""
                        program_hbase = self.persist_util.GetObjectFromHbase(
                            self.param_spec["hbase_all"],
                            ["cf:doc_type"],
                            [data["mgdbId"][4:]],
                        )
                        if (
                            program_hbase
                            and program_hbase[data["mgdbId"][4:]]
                            .get(b"cf:doc_type", b"")
                            .decode()
                            == "newLive"
                        ):
                            item = {
                                "doc_id": data["mgdbId"][4:],
                                "status": 0,
                                "additional_status": 0,
                                "update_time": int(time.time()),
                            }
                            self.persist_util.SendToKafka([item])
                            self.persist_util.SendToHbase(
                                self.param_spec["hbase_all"], [item]
                            )
                            print(
                                "直播间下线状态更新HBASE成功!",
                                data["mgdbId"],
                                program_hbase,
                                item,
                            )
                            self.log.logger.info(
                                "kafka下线二台直播间消息:"
                                + str(program_hbase)
                                + "\t"
                                + str(item)
                            )
                            self.prod1.Set(
                                "video_kafka_newlive_poff",
                                "monitor",
                                True,
                                12 * 60 * 60,
                            )

                        """更新hbase-latest"""
                        try:
                            program_hbase = self.persist_util.GetObjectFromHbase(
                                self.param_spec["hbase_pendant_latest"],
                                ["cf:doc_type"],
                                [data["mgdbId"][4:]],
                            )
                            if (
                                program_hbase
                                and program_hbase[data["mgdbId"][4:]]
                                .get(b"cf:doc_type", b"")
                                .decode()
                                == "newLive"
                            ):
                                item = {
                                    "doc_id": data["mgdbId"][4:],
                                    "status": 0,
                                    "additional_status": 0,
                                }
                                self.persist_util.SendToHbase(
                                    self.param_spec["hbase_pendant_latest"], [item]
                                )
                                print(
                                    "挂件下线状态更新HBASE成功!",
                                    data["mgdbId"],
                                    program_hbase,
                                    item,
                                )
                        except Exception as e:
                            print(e)

                        """更新内容画像hbase"""
                        try:
                            item = {
                                "doc_id": data["mgdbId"][4:],
                                "status": 0,
                                "additional_status": 0,
                            }
                            self.persist_util.SendToHbase(
                                self.param_spec["hbase_cp"], [item]
                            )
                            print("挂件下线状态更新HBASE-CP成功!", data["mgdbId"], item)
                        except Exception as e:
                            print(e)

                        """更新搜推ES"""
                        try:
                            program = self.persist_util.GetProgramFromES(
                                self.param_spec["es_byte_idx"], data["mgdbId"][4:]
                            )
                            if program and program.get("doc_type", "") == "newLive":
                                res = self.es.update(
                                    index=self.param_spec["es_byte_idx"],
                                    id=data["mgdbId"][4:],
                                    body={"doc": {"status": 0, "additional_status": 0}},
                                )
                                print(
                                    "直播间下线状态更新搜推ES成功!",
                                    data["mgdbId"],
                                    program,
                                    res,
                                )
                        except Exception as e:
                            print(e)

                        """更新redis by http"""
                        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
                        post_data = {}
                        post_data["operatingUser"] = "zhangcong_yy"
                        post_data["dataType"] = "zset"
                        post_data["operateType"] = "write"
                        post_data["addData"] = {
                            "key": "video_search_recomm_content_offline",
                            "valueZSet": {data["mgdbId"][4:]: int(obj["version"])},
                        }
                        print(json.dumps(post_data, ensure_ascii=False))
                        readed = REQ.post(url=url, json=post_data)
                        print("二台下线状态更新HTTP成功!", readed.json())

                        """更新redis by http"""
                        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
                        post_data = {}
                        post_data["operatingUser"] = "zhangcong_yy"
                        post_data["dataType"] = "hash"
                        post_data["operateType"] = "query"
                        post_data["args"] = "exists video_cp:" + data["mgdbId"][4:]
                        readed = REQ.post(url=url, json=post_data)
                        print("query:", readed.json(), type(readed.json()))

                        if readed.json()["data"] == 1:
                            post_data = {}
                            post_data["operatingUser"] = "zhangcong_yy"
                            post_data["operateType"] = "del"
                            post_data["delData"] = {
                                "key": "video_cp:" + data["mgdbId"][4:]
                            }
                            readed = REQ.post(url=url, json=post_data)
                            print("下线redis key删除成功!", readed.json())
            except Exception as e:
                self.log.logger.info("处理异常:" + str(e) + "\t" + str(msg))
                continue

        return None

    def ConsumerOndemandKafka(
        self,
    ):
        """
        消费点播消息
        """
        bootstrap_servers = self.param_spec["kafka_ondemand"]["bootstrap_servers"]
        topic_rec = self.param_spec["kafka_ondemand"]["topic_rec"]
        group_rec = "consume_ondemand_group"
        #        self.consumer = Kafka_consumer(bootstrap_servers, topic_rec, group_rec, auto_offset_reset='earliest')
        self.consumer = Kafka_consumer(
            bootstrap_servers, topic_rec, group_rec, auto_offset_reset="latest"
        )

        for msg in self.consumer.consume_data():
            try:
                obj = json.loads(msg.value.decode("utf8"))
                if obj.get("type", "") not in ["208", "217"]:
                    continue

                if obj["type"] == "217":
                    if int(obj["operation"]) != 2 and obj["data"].get(
                        "productInfoPackageId", ""
                    ) not in ["141"]:
                        """非视讯产品线节目丢弃"""
                        #                        print (obj['type'], obj['data'])
                        continue
                else:
                    if int(obj["operation"]) != 2 and obj["data"].get(
                        "content", {}
                    ).get("PRODUCT_INFO_PACKAGE_ID", "") not in ["1"]:
                        """非视讯产品线节目丢弃"""
                        #                        print (obj['type'], obj['data'])
                        continue

                rkey = "ondemand_version:" + obj["id"]
                rvalue = self.prod1.Get(rkey)
                if rvalue:
                    if int(obj["version"]) < int(rvalue):
                        continue

                if int(obj["operation"]) == 2:
                    print(
                        "节目下线:",
                        obj["type"],
                        obj["id"],
                        obj["operation"],
                        obj["version"],
                        type(obj["version"]),
                        obj["time"],
                    )
                    """更新hbase"""
                    program_hbase = self.persist_util.GetObjectFromHbase(
                        self.param_spec["hbase_all"], ["cf:doc_type"], [obj["id"]]
                    )
                    if program_hbase and program_hbase[obj["id"]].get(
                        b"cf:doc_type", b""
                    ).decode() not in ["newLive", "pendant"]:
                        item = {
                            "doc_id": obj["id"],
                            "status": 0,
                            "additional_status": 0,
                            "update_time": int(time.time()),
                        }
                        self.persist_util.SendToKafka([item])
                        self.persist_util.SendToHbase(
                            self.param_spec["hbase_all"], [item]
                        )
                        print(
                            "点播下线状态更新HBASE成功!", obj["id"], program_hbase, item
                        )
                        self.log.logger.info(
                            "kafka下线点播消息:" + str(program_hbase) + "\t" + str(item)
                        )
                        self.prod1.Set(
                            "video_kafka_ondemand_poff", "monitor", True, 12 * 60 * 60
                        )

                    """更新hbase-latest"""
                    try:
                        program_hbase = self.persist_util.GetObjectFromHbase(
                            self.param_spec["hbase_ondemand_latest"],
                            ["cf:doc_type"],
                            [obj["id"]],
                        )
                        if program_hbase and program_hbase[obj["id"]].get(
                            b"cf:doc_type", b""
                        ).decode() not in ["newLive", "pendant"]:
                            item = {
                                "doc_id": obj["id"],
                                "status": 0,
                                "additional_status": 0,
                                "update_time": int(time.time()),
                            }
                            self.persist_util.SendToHbase(
                                self.param_spec["hbase_ondemand_latest"], [item]
                            )
                            print(
                                "点播下线状态更新HBASE成功!",
                                obj["id"],
                                program_hbase,
                                item,
                            )
                    except Exception as e:
                        print(e)

                    """更新内容画像hbase"""
                    try:
                        item = {
                            "doc_id": obj["id"],
                            "status": 0,
                            "additional_status": 0,
                            "update_time": int(time.time()),
                        }
                        self.persist_util.SendToHbase(
                            self.param_spec["hbase_cp"], [item]
                        )
                        print("点播下线状态更新HBASE-CP成功!", obj["id"], item)
                    except Exception as e:
                        print(e)

                    """更新搜推ES"""
                    try:
                        program_es = self.persist_util.GetProgramFromES(
                            self.param_spec["es_byte_idx"], obj["id"]
                        )
                        if program_es and program_es.get("doc_type", "") not in [
                            "newLive",
                            "pendant",
                        ]:
                            res = self.es.update(
                                index=self.param_spec["es_byte_idx"],
                                id=obj["id"],
                                body={"doc": {"status": 0, "additional_status": 0}},
                            )
                            print(
                                "点播下线状态更新搜推ES成功!",
                                obj["id"],
                                program_es,
                                res,
                            )
                    except Exception as e:
                        print(e)

                    #                    """更新redis by http"""
                    #                    url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
                    #                    post_data = {}
                    #                    post_data["operatingUser"] = "zhangcong_yy"
                    #                    post_data["dataType"] = "zset"
                    #                    post_data["operateType"] = "write"
                    #                    post_data["addData"] = {"key":"video_search_recomm_content_offline", "valueZSet":{obj['id']:int(obj['version'])}}
                    #                    print (json.dumps(post_data, ensure_ascii=False))
                    #                    readed = REQ.post(url=url, json=post_data)
                    #                    print ("点播下线状态更新HTTP成功!", readed.json())

                    """更新redis by http"""
                    url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
                    post_data = {}
                    post_data["operatingUser"] = "zhangcong_yy"
                    post_data["dataType"] = "hash"
                    post_data["operateType"] = "query"
                    post_data["args"] = "exists video_cp:" + obj["id"]
                    readed = REQ.post(url=url, json=post_data)
                    print("query:", readed.json(), type(readed.json()))

                    if readed.json()["data"] == 1:
                        post_data = {}
                        post_data["operatingUser"] = "zhangcong_yy"
                        post_data["operateType"] = "del"
                        post_data["delData"] = {"key": "video_cp:" + obj["id"]}
                        readed = REQ.post(url=url, json=post_data)
                        print("下线redis key删除成功!", readed.json())
                else:
                    print(
                        "节目发布 or 更新:",
                        obj["type"],
                        obj["id"],
                        obj["operation"],
                        obj["version"],
                        type(int(obj["version"])),
                        obj["time"],
                    )
                self.prod1.Set(rkey, int(obj["version"]), True, 7 * 24 * 60 * 60)

            except Exception as e:
                print("处理异常:" + str(e) + "\t" + msg.value.decode("utf8"))
                self.log.logger.info(
                    "处理异常:" + str(e) + "\t" + msg.value.decode("utf8")
                )
                continue

        return None

    def ConsumerDebugKafka(
        self,
    ):
        """
        消费debug消息
        """
        bootstrap_servers = self.param_spec["kafka_ondemand"]["bootstrap_servers"]
        topic_rec = "videoQualityLabelRecommend"
        group_rec = "test00"
        self.consumer = Kafka_consumer(
            bootstrap_servers, topic_rec, group_rec, auto_offset_reset="earliest"
        )

        cnt = 0
        for msg in self.consumer.consume_data():
            try:
                obj = json.loads(msg.value.decode("utf8"))
                print(obj)

                cnt += 1
                if cnt > 5:
                    break
            except Exception as e:
                print("处理异常:" + str(e) + "\t" + msg.value.decode("utf8"))
                self.log.logger.info(
                    "处理异常:" + str(e) + "\t" + msg.value.decode("utf8")
                )
                continue
        return None

    def UpdateRemoteRedis(
        self,
    ):
        #        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
        #        url = "http://10.194.140.46:8655/redis/data/operate"
        #        post_data = {}
        #        post_data["operatingUser"] = "zhangcong_yy"
        #        post_data["dataType"] = "zset"
        #        post_data["operateType"] = "write"
        #        post_data["addData"] = {"key":"video_search_recomm_content_offline", "valueZSet":{"943859124":1757386289000, "943859125":1757386289000}}
        #        readed = REQ.post(url=url, json=post_data)
        #        print ("点播下线状态更新HTTP成功!", readed.json())

        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
        #        url = "http://10.194.140.46:8655/redis/data/operate"
        post_data = {}
        post_data["operatingUser"] = "zhangcong_yy"
        post_data["dataType"] = "zset"
        post_data["operateType"] = "query"
        post_data["args"] = "zrange video_search_recomm_content_offline 0 -1 WITHSCORES"
        readed = REQ.post(url=url, json=post_data)
        print("query:", readed.json(), type(readed.json()))

        keys, values = [], []
        for index, value in enumerate(readed.json()["data"]):
            if index % 2 == 0:
                keys.append(value)
            else:
                values.append(value)
        valid_data = {}
        timestamp = int(time.time()) * 1000
        for k, v in dict(zip(keys, values)).items():
            if int(v) < timestamp - 2 * 60 * 60 * 1000:
                print("过期数据:", k, v)
            else:
                valid_data[k] = int(v)
        print(valid_data)

        """更新redis by http"""
        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
        #        url = "http://10.194.140.46:8655/redis/data/operate"
        post_data = {}
        post_data["operatingUser"] = "zhangcong_yy"
        post_data["operateType"] = "del"
        post_data["delData"] = {"key": "video_search_recomm_content_offline"}
        readed = REQ.post(url=url, json=post_data)
        print("下线redis key删除成功!", readed.json())

        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
        #        url = "http://10.194.140.46:8655/redis/data/operate"
        post_data = {}
        post_data["operatingUser"] = "zhangcong_yy"
        post_data["dataType"] = "zset"
        post_data["operateType"] = "write"
        post_data["addData"] = {
            "key": "video_search_recomm_content_offline",
            "valueZSet": valid_data,
        }
        readed = REQ.post(url=url, json=post_data)
        print("点播下线状态更新HTTP成功!", readed.json())

        url = "http://10.194.164.67:8090/tsgrd/redis/data/operate"
        #        url = "http://10.194.140.46:8655/redis/data/operate"
        post_data = {}
        post_data["operatingUser"] = "zhangcong_yy"
        post_data["dataType"] = "zset"
        post_data["operateType"] = "query"
        post_data["args"] = "zrange video_search_recomm_content_offline 0 -1 WITHSCORES"
        readed = REQ.post(url=url, json=post_data)
        print("query:", readed.json(), type(readed.json()))

        return None


if "__main__" == __name__:
    parser = ap.ArgumentParser()
    parser.add_argument("--func_type", default="")
    parser.add_argument("--target_dir", default="")
    parser.add_argument("--param_spec", required=True)
    args = parser.parse_args()
    with open(args.param_spec) as f:
        param_spec = json.load(f)
    param_spec.update(args.__dict__)

    obj = CalculateContentStatus(param_spec)
    if args.func_type == "pendant":
        obj.ConsumerPendantKafka()
    elif args.func_type == "ondemand":
        obj.ConsumerOndemandKafka()
    elif args.func_type == "refresh":
        obj.UpdateRemoteRedis()
