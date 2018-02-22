# -*- coding:utf-8 -*-
# 腾讯NBA数据库
# 季前赛、常规赛

import json
import requests
import pymysql


def spyider(datastart, dataend):
    url = "http://matchweb.sports.qq.com/kbs/list?from=NBA_PC&columnId=100000&startTime=" + datastart + "&endTime=" + dataend + "&callback=ajaxExec&_=1516417087848"
    data = requests.get(url).text
    str1 = data.split('(')
    str2 = str1[1].split(')')
    js = json.loads(str2[0])
    data = js["data"]
    try:
        for item in data:
            for value in data[item]:
                matchDesc = value["matchDesc"]  # 比赛类型
                starttime = value["startTime"]  # 开始时间
                leftpng = value["leftBadge"]  # 左边球队图标
                leftGoal = value["leftGoal"]  # 左边球队得分
                leftName = value["leftName"]  # 左边球队名字
                leftId = value["leftId"]  # 左边球队编号
                rightpng = value["rightBadge"]  # 右边球队图标
                rightGoal = value["rightGoal"]  # 右边球队得分
                rightName = value["rightName"]  # 右边球队名字
                rightId = value["rightId"]  # 右边球队编号
                week = value["week"]  # 对应星期
                midtemp1 = value["mid"]  # 本场比赛详细信息
                midtemp2 = midtemp1.split(":")
                mid = midtemp2[1]
                # mid_url="http://nba.stats.qq.com/nbascore/?mid="+mid+""#本场比赛详细数据url
                detial(mid)
                # game表
                left_sql_game = "insert into game(game_matchDesc,game_starttime,game_teampng,game_teamGoal,game_teamName,game_teamId,game_week,game_mid_num) values('" + matchDesc + "','" + starttime + "','" + leftpng + "','" + leftGoal + "','" + leftName + "','" + leftId + "','" + week + "','" + mid + "');"
                right_sql_game = "insert into game(game_matchDesc,game_starttime,game_teampng,game_teamGoal,game_teamName,game_teamId,game_week,game_mid_num) values('" + matchDesc + "','" + starttime + "','" + rightpng + "','" + rightGoal + "','" + rightName + "','" + rightId + "','" + week + "','" + mid + "');"
                print("left_sql_game  :" + left_sql_game)
                print("right_sql_game  :" + right_sql_game)
                with connection.cursor() as cursor:
                    cursor.execute(left_sql_game)
                    cursor.execute(right_sql_game)
                    connection.commit()
    except Exception as e:
        print(e)
        # raise e


def detial(mid):
    url = "http://sportswebapi.qq.com/kbs/matchStat?from=nba_database&selectParams=teamRank,periodGoals,playerStats,nbaPlayerMatchTotal,maxPlayers&mid=100000:" + str(
        mid) + "&callback=jQuery1113019037088317220197_1516434681633&_=1516434681634"
    data = requests.get(url).text
    str1 = data.split("(")
    str2 = ""
    for i in range(len(str1)):
        if i > 0:
            str2 += str1[i]

    str3 = str2.split(")")
    index = 0
    for i in str3:
        if i != "":
            index += 1
    str4 = ""
    for i in range(index):
        if i <= index - 1:
            str4 += str3[i]
    js = json.loads(str4)
    injs = js["data"]
    try:
        # teamInfo
        teamInfo = injs["teamInfo"]
        startTime = teamInfo["startTime"]  # 开始时间
        leftEnName = teamInfo["leftEnName"]  # 左边队伍英文名字
        leftFullCnName = teamInfo["leftFullCnName"]  # 左边队伍全名
        leftId = teamInfo["leftId"]  # 左边队伍编号
        leftRank = teamInfo["leftRank"]  # 左边排行
        leftRecord = teamInfo["leftRecord"]  # 左边队伍排名
        rightEnName = teamInfo["rightEnName"]  # 右边队伍英文名字
        rightFullCnName = teamInfo["rightFullCnName"]  # 右边队伍全名
        rightId = teamInfo["rightId"]  # 右边队伍编号
        rightRank = teamInfo["rightRank"]  # 左边排行
        rightRecord = teamInfo["rightRecord"]  # 左边队伍排名
        playerStats = injs["playerStats"]

        # teaminfo表
        left_sql_teaminfo = "insert into teaminfo(game_startTime,EnName,FullCnName,Id,Rank,Record,mid_num) values('" + startTime + "','" + leftEnName + "','" + leftFullCnName + "','" + leftId + "','" + leftRank + "','" + leftRecord + "','" + mid + "');"
        right_sql_teaminfo = "insert into teaminfo(game_startTime,EnName,FullCnName,Id,Rank,Record,mid_num) values('" + startTime + "','" + rightEnName + "','" + rightFullCnName + "','" + rightId + "','" + rightRank + "','" + rightRecord + "','" + mid + "');"
        print("left_sql_teaminfo  :" + left_sql_teaminfo)
        print("right_sql_teaminfo  :" + right_sql_teaminfo)
        with connection.cursor() as cursor:
            cursor.execute(left_sql_teaminfo)
            cursor.execute(right_sql_teaminfo)
            connection.commit()

        # left
        left = playerStats["left"]
        right = playerStats["right"]
        left_index = 0
        for i in left:
            if i != "":
                left_index += 1
        for item in range(1, left_index):
            # leftId
            name = left[item]["row"][0]  # 球员名字
            isshoufa = left[item]["row"][1]  # 是否首发
            time = left[item]["row"][2]  # 时间
            mark = left[item]["row"][3]  # 得分
            lanban = left[item]["row"][4]  # 篮板
            zhugong = left[item]["row"][5]  # 助攻
            toulan = left[item]["row"][6]  # 投篮
            sanfen = left[item]["row"][7]  # 三分
            faqiu = left[item]["row"][8]  # 罚球
            qianchangban = left[item]["row"][9]  # 前场板
            houchangban = left[item]["row"][10]  # 后场板
            qiangduan = left[item]["row"][11]  # 抢断
            gaimao = left[item]["row"][12]  # 盖帽
            shiwu = left[item]["row"][13]  # 失误
            fangui = left[item]["row"][14]  # 犯规
            ########################################################################################
            # leftId 加队伍编号
            # player表
            left_sql_players = "insert into players(mid_num,Id,name,isshoufa,time,mark,lanban,zhugong,toulan,sanfen,faqiu,qianchangban,houchangban,qiangduan,gaimao,shiwu,fangui) values('" + mid + "','" + leftId + "','" + name + "','" + isshoufa + "','" + time + "','" + mark + "','" + lanban + "','" + zhugong + "','" + toulan + "','" + sanfen + "','" + faqiu + "','" + qianchangban + "','" + houchangban + "','" + qiangduan + "','" + gaimao + "','" + shiwu + "','" + fangui + "');"
            print("left_sql_players  :" + left_sql_players)

            with connection.cursor() as cursor:
                cursor.execute(left_sql_players)
                connection.commit()

        ########################################################################################

        # right
        right = playerStats["right"]
        right_index = 0
        for i in right:
            if i != "":
                right_index += 1
        for item in range(1, right_index):
            # rightId
            name = right[item]["row"][0]  # 球员名字
            isshoufa = right[item]["row"][1]  # 是否首发
            time = right[item]["row"][2]  # 时间
            mark = right[item]["row"][3]  # 得分
            lanban = right[item]["row"][4]  # 篮板
            zhugong = right[item]["row"][5]  # 助攻
            toulan = right[item]["row"][6]  # 投篮
            sanfen = right[item]["row"][7]  # 三分
            faqiu = right[item]["row"][8]  # 罚球
            qianchangban = right[item]["row"][9]  # 前场板
            houchangban = right[item]["row"][10]  # 后场板
            qiangduan = right[item]["row"][11]  # 抢断
            gaimao = right[item]["row"][12]  # 盖帽
            shiwu = right[item]["row"][13]  # 失误
            fangui = right[item]["row"][14]  # 犯规
            ########################################################################################
            # rightId 加队伍编号
            # player表
            right_sql_players = "insert into players(mid_num,Id,name,isshoufa,time,mark,lanban,zhugong,toulan,sanfen,faqiu,qianchangban,houchangban,qiangduan,gaimao,shiwu,fangui) values('" + mid + "','" + rightId + "','" + name + "','" + isshoufa + "','" + time + "','" + mark + "','" + lanban + "','" + zhugong + "','" + toulan + "','" + sanfen + "','" + faqiu + "','" + qianchangban + "','" + houchangban + "','" + qiangduan + "','" + gaimao + "','" + shiwu + "','" + fangui + "');"
            print("right_sql_players  :" + right_sql_players)
            with connection.cursor() as cursor:
                cursor.execute(right_sql_players)
                connection.commit()
    ########################################################################################

    except Exception as e:
        print(e)
        # raise e


if __name__ == '__main__':
    datastart = "2017-09-30"
    dataend = "2018-01-20"

    config = {  # mysql连接配置
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': '1234',
        'db': 'nba',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
    }
    connection = pymysql.connect(**config)  # 获取连接

    spyider(datastart, dataend)

    print("完成")
    connection.close();
