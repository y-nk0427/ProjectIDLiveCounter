import asyncio
import csv
import pandas
import traceback
import aiohttp
from datetime import datetime, timedelta
# import ntplib
import math
import scratchattach as s3
from os import path
import json
import warnings
# import time

async def main():
    async with aiohttp.ClientSession() as session:  # セッションをすべての場所で共有
        while True:  # エラーリトライ用無限ループ
            try:
                warnings.simplefilter('ignore', DeprecationWarning)
                
                # ファイルが存在するフォルダーのパス
                DIR_PATH = path.dirname(__file__)

                # タイムサーバーから時刻を取得 (ここで通信エラーが起こったりするのでやめました)
                # ntp_client = ntplib.NTPClient()
                # ntp_response = ntp_client.request('pool.ntp.org', version=3)
                # ntp_time = datetime.utcfromtimestamp(ntp_response.tx_time)
                # device_ntp_diff = datetime.utcnow() - ntp_time
                #
                # print(f"サーバー時刻のずれ: {timedelta.total_seconds(device_ntp_diff)}s")

                # 時刻を取得する関数
                def utcnow():
                    # nonlocal device_ntp_diff
                    return datetime.utcnow()  # + device_ntp_diff

                # セッションIDをファイルから読み取り
                with open(path.join(DIR_PATH, "session_id.txt"), "r") as file:
                    s3_session_id = file.read().strip()

                # セッション情報 (Scratchの方のクラウド変数が落ちているのでコメントアウト)
                s3_session = s3.Session(s3_session_id, username="y_nk")
                # s3_session.connect_cloud(1064835367)
                s3_connection_tw = s3_session.connect_tw_cloud(1064835367, purpose="Project ID Live Counter Bot v2.0 by @y_nk - Using Python & scratchattach library v1.7.3", contact="yoheinz2010@gmail.com")

                async def set_var(name, value):
                    #return
                    err = 0

                    for err_cnt in range(15):
                        try:
                            s3_connection_tw.set_var(name, value)
                            # s3_connection.set_var(name, value)
                            await asyncio.sleep(0.1)
                            return
                        
                        except Exception as e:
                            err = e
                            print(f"Error while setting value to cloud variable... name: {name}, count: {err_cnt}, err: {e!r}")
                            await asyncio.sleep(1)

                    raise err

                print("接続完了: 推定ID調査開始")

                # ファイルに記録されたデータを読み取り
                with open(path.join(DIR_PATH, "data.json"), "r") as file:
                    data = json.load(file)

                with open(path.join(DIR_PATH, "speed_data.csv"), "r") as file:
                    long_term_speed_data = list(csv.reader(file))

                max_id = 0  # 現在の最大ID
                num_of_projects_to_monitor = 300  # 1度のリクエストで取得するID数

                # サーバの再起動時に大まかな現在の最大IDを調べる
                i = data["max_id"]
                print(f"#{i}...")

                steps = [1000000, 100000, 10000, 1000, 100]
                for step in steps:
                    while True:
                        max_id = i
                        is_shared_exists = False  # 送ったリクエストの中に共有済みプロジェクトのものがあるか？

                        tasks = [
                            asyncio.create_task(session.get(f"https://uploads.scratch.mit.edu/get_image/project/{id}_1x1.png"))
                            for id in range(i, i + num_of_projects_to_monitor)
                        ]

                        # 全タスクの並行処理
                        responses = await asyncio.gather(*tasks)

                        for id, response in zip(range(i, i + num_of_projects_to_monitor), responses):
                            shared = response.status == 200

                            # 最大IDの更新
                            if shared and id > max_id:
                                max_id = id
                                is_shared_exists = True

                        # 共有済みのものが見つからない場合、ステップを減少
                        if not is_shared_exists:
                            i -= step
                            break

                        i += step

                    print(f"#{i}...")

                max_id = i

                print("更新開始")

                # 定義地獄 (何も整理されていない)
                NUM_OF_MILESTONES_TO_MONITOR = 4  # 監視するキリ番プロジェクトの数
                before_max_id = max_id  # 前回更新時の最大ID
                recent_speed_list = []  # 直近50更新で算出されたプロジェクト作成速度
                recent_update_list_for_stats = []  # 直近50更新での {現在の時刻, 現在の最大ID} を保存したリスト (統計データ用速度算出用)
                long_term_speed_data = pandas.read_csv(path.join(DIR_PATH, "speed_data.csv"), header=None).values.tolist()  # 7日周期1時間ごとに記録されたプロジェクト作成速度の平均データ
                before_before_milestones = data["before_milestones"]  # プログラム実行前に記録されていた過去のキリ番の情報
                before_update_num = 0  # 前回の更新でのプロジェクトの作成時刻
                before_milestones = []  # 過去のキリ番の情報
                next_milestone = 0  # 次のキリ番ID
                data_string = ""  # クラウド変数に渡す用の数字だけの動的なデータ文字列
                milestone_string = ""  # 同様に、キリ番情報の文字列
                speed_data_string = ""  # 同様に、その曜日のプロジェクト作成速度情報の文字列
                num_of_checks = 0  # 更新回数
                last_update = 0  # 現時点での最大IDの取得時刻
                last_update_num = 0  # ↑の数値版
                last_update_minus_30min = 0  # last_update の30分前 (speed_data格納時に使用)
                day_week_idx = 0  # 0 (月曜0時) - 167 (日曜23時)
                last_check = 0  # 今ループでの情報取得開始時刻
                last_check_num = 0  # ↑の数値版
                num_of_projects_to_monitor = 1000  # この更新で調べるプロジェクトの数
                create_speed = 0  # 1更新での作成速度 (コンソール表示用のみ)
                create_speed_last50 = 0  # 直近50更新の平均作成速度
                create_speed_last10 = 0  # 直近50更新の平均作成速度
                create_speed_for_stats = 0  # 50更新前での情報から求められた作成速度 (統計データ用)
                id_diff = 0  # 前回の更新からのIDの増加
                time_diff = 0  # 前回の更新から経った時間
                unfound_streak = 0  # 更新がなかった連続数
                num_of_updates_gonna_be_ignored = 0  # ↑が大きくなっ手から更新された時に、この回数分は last_update や num_of_projects_to_monitor をリセットしない
                
                DATE_2000 = datetime(2000, 1, 1, 0, 0, 0)  # 2000年からの秒数に変換する用

                MILESTONE_STEP = int(1e7)  # キリ番のステップ
                SPECIAL_MILESTONES = [1111111111, 1234567890]  # キリが良いわけではない特別なキリ番

                # next_milestone を算出
                next_milestone = int((math.ceil(max_id / MILESTONE_STEP)) * MILESTONE_STEP)

                if any([max_id < e < next_milestone for e in SPECIAL_MILESTONES]):
                    next_milestone = [e for e in SPECIAL_MILESTONES if max_id < e < next_milestone][0]

                # 文字列を数字の羅列に変換する関数
                def convert_username_to_numstr(str):
                    CHAR_LIST = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_"

                    result = ""
                    for char in str:
                        result += f"{CHAR_LIST.find(char) + 1:02}"

                    # 固定長にするために40文字まで末尾を0埋め
                    return f"{result:040}"
                
                # 日付か日付文字列(ISO 8601)を2000年からの秒数に変換する関数 (Scratchで扱いやすく／クラウド変数に送る ため)
                def convert_datestr_to_num(date):
                    if type(date) == datetime:
                        return (date - DATE_2000).total_seconds()
                    
                    elif type(date) == str:
                        return (datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ") - DATE_2000).total_seconds()

                # before_milestones を最新の情報に更新
                temp_id = max_id
                for i in range(NUM_OF_MILESTONES_TO_MONITOR):
                    milestone = int((math.floor(temp_id / MILESTONE_STEP)) * MILESTONE_STEP)

                    if any([temp_id > e > milestone for e in SPECIAL_MILESTONES]):
                        milestone = [e for e in SPECIAL_MILESTONES if temp_id > e > milestone][0]

                    # もし before_before_milestones に該当のキリ番情報(共有済みの)がすでに載っているなら、
                    # 新たに情報の取得はせずに元から載っていた情報を使用する
                    if any([e["id"] == milestone and e["shared"] for e in before_before_milestones]):  # 共有 → 共有or非共有
                        before_milestones.append([e for e in before_before_milestones if e["id"] == milestone][0])

                    else:
                        response = await session.get(f"https://api.scratch.mit.edu/projects/{milestone}")

                        shared = response.status == 200

                        if shared:  # データなしor非共有 → 共有
                            res_json = await response.json()
                            creator = res_json["author"]["username"]
                            created_time = int(convert_datestr_to_num(res_json["history"]["created"]))

                            # 取ってきたデータを before_milestones に追加
                            before_milestones.append({"id": milestone, "shared": shared, "creator": creator, "created_time": created_time})
                        
                        else:
                            # 非共有 → 非共有の場合も、新たに情報の取得はせずに元から載っていた情報を使用する
                            if any([e["id"] == milestone and not e["shared"] for e in before_before_milestones]):  # 非共有 → 非共有
                                before_milestones.append([e for e in before_before_milestones if e["id"] == milestone][0])

                            else:  # データなし → 非共有
                                creator = None
                                created_time = 0

                                # 取ってきたデータを before_milestones に追加
                                before_milestones.append({"id": milestone, "shared": shared, "creator": creator, "created_time": created_time})

                    if before_milestones[i]["shared"]:
                        milestone_string += f"{before_milestones[i]["id"]}1{convert_username_to_numstr(before_milestones[i]["creator"])}{before_milestones[i]["created_time"]:010}"
                    else:
                        milestone_string += f"{before_milestones[i]["id"]}0{0:040}{before_milestones[i]["created_time"]:010}"

                    # [どのID以内で一番小さいキリ番について調べるか] を [今調べたキリ番] から1下げることで、次ループでひとつ前のキリ番を調べることになる
                    temp_id = milestone - 1

                # data に格納
                data["before_milestones"] = before_milestones

                # data を書き込み
                with open(path.join(DIR_PATH, "data.json"), "w") as file:
                    json.dump(data, file, indent=4)

                await set_var("milestone_data", milestone_string)

                before_update_num = convert_datestr_to_num(utcnow())

                ###################################################################### ここから無限ループ ######################################################################

                while True:
                    max_checked_id = max_id

                    async def check_if_project_exists(session, id):
                        nonlocal max_checked_id, max_id, last_update

                        try:
                            async with session.get(f"https://uploads.scratch.mit.edu/get_image/project/{id}_1x1.png") as response:
                                if max_checked_id < id:
                                    max_checked_id = id

                                if response.status == 200:
                                    if max_id < id:
                                        max_id = id
                                        last_update = utcnow()

                                elif response.status != 688:
                                    print(f"A task responsed with a status {response.status}.")
                        
                        except Exception as e:
                            return e
                        
                    # ここら辺に散らばってるコメントアウトされたコード(##始まり)は、v2.1e のデバッグ用に使用されたものです。
                    ## last_check = utcnow()
                    ## last_check_num = convert_datestr_to_num(last_check)

                    ## if not 40 < num_of_checks < 60:

                    for err_cnt in range(1500000000000000):  # エラーリトライ用
                        last_check = utcnow()
                        last_check_num = convert_datestr_to_num(last_check)

                        if num_of_checks > 2:
                            # 今回調べるプロジェクトの数を作成速度と1ループの時間から決定
                            num_of_projects_to_monitor = max(int(max(create_speed_last10, create_speed_last50) * (last_check_num - before_update_num) * 2), 15)

                        # 通常取得枠
                        tasks = [
                            asyncio.create_task(check_if_project_exists(session, i)) for i in range(max_id, max_id + num_of_projects_to_monitor)
                        ]

                        # キリ番取得枠
                        tasks_milestones = [
                            asyncio.create_task(session.get(f"https://api.scratch.mit.edu/projects/{elem['id']}"))
                            for elem in before_milestones
                        ]

                        responses = await asyncio.gather(*tasks_milestones, *tasks, return_exceptions=True)  # 並行して取得

                        for response in responses:  # エラーを返したタスクが無いか確認していく
                            if isinstance(response, Exception):
                                if err_cnt >= 100:  # 10連続でエラーが発生したなら、応答(エラー)をraise
                                    raise response
                                
                                print(f"Error while getting data... count: {err_cnt}, err: {response!r}")
                                await asyncio.sleep(1)
                                break  # 10連続以下の場合、内側のforを抜けて、外側のforをもう1回やり直す
                        else:
                            break  # 正常終了(エラーが見つからない=breakされず)したなら、外側のforをbreakしてそのまま進む

                    # キリ番監視枠のレスポンス処理
                    for i, response in zip(range(NUM_OF_MILESTONES_TO_MONITOR), responses[:NUM_OF_MILESTONES_TO_MONITOR]):
                        if before_milestones[i]["shared"]:
                            continue
                        
                        shared = response.status == 200
                        if shared and before_milestones[i]["shared"] == False:

                            res_json = await response.json()
                            before_milestones[i]["shared"] = True
                            before_milestones[i]["creator"] = res_json["author"]["username"]
                            before_milestones[i]["created_time"] = int(convert_datestr_to_num(res_json["history"]["created"]))

                            print(f"\n[{last_check}] <<< Milestone #{before_milestones[0]["id"]} has just been shared!! (Creator: @{before_milestones[i]["creator"]}) >>>\n")

                            # 新たに共有されたものが見つかった場合、milestone_string を組み立てなおす
                            milestone_string = ""
                            for j in range(NUM_OF_MILESTONES_TO_MONITOR):
                                if before_milestones[j]["shared"]:
                                    milestone_string += f"{before_milestones[j]['id']}1{convert_username_to_numstr(before_milestones[j]["creator"])}{int(before_milestones[j]["created_time"]):010}"
                                else:
                                    milestone_string += f"{before_milestones[j]['id']}0{0:040}{int(before_milestones[j]["created_time"]) if before_milestones[j]["created_time"] != None else 0:010}"
                                                        
                            await set_var("milestone_data", milestone_string)
                            await set_var("milestone_data", milestone_string)
                            await set_var("milestone_data", milestone_string)
                    # 情報取得終了

                    ## else: time.sleep(0.1)

                    if max_id != before_max_id:  # 最大IDの更新があったら
                        if num_of_checks >= 2:  # 無限ループ前の推定では最新のものに追いついてない可能性があるので、詳細な計算は2回の更新後に始める

                            # n連続で新しいプロジェクトが見つからなかった場合、恐らくScratchサーバー又はこちら側の不調と考える。
                            # 不調の後に新しいプロジェクトが見つかった場合、素直に更新すると、不調の間に実は作成されていたプロジェクトが短時間で大放出されて、
                            #  作成速度がめちゃくちゃ跳ね上がってしまうので、それを防ぐためにそこからm回は何もなかったことにする
                            # v2.1g にて、(n, m) を (8, 4) から (5, 10) に変更しました (まだ事故っていたため)
                            if unfound_streak >= 5:
                                num_of_updates_gonna_be_ignored = 10
                            
                            unfound_streak = 0

                            if num_of_updates_gonna_be_ignored > 0:
                                num_of_updates_gonna_be_ignored -= 1

                                print(f"[#{num_of_checks:06}: {last_check}]  ~  #{max_id} |{max_id - before_max_id:3}p /{last_check_num - last_update_num:6.3f}s = {(max_id - before_max_id) / (last_check_num - last_update_num):7.3f} p/s                                                   *{num_of_projects_to_monitor:3}")


                            else:
                                id_diff = max_id - before_max_id
                                last_update = last_check
                                last_update_num = last_check_num
                            
                                time_diff = (last_check_num - before_update_num)
                                create_speed = id_diff / time_diff

                                # 今回の更新で、調べたプロジェクトの中でほぼすべてが存在していた場合、Scratch側の不調からの急な回復が考えられるので、
                                # 作成速度の不本意な急増を抑えるべく一旦見なかったことにする
                                #  → v2.1e にてこの機能はなくなりました (新しい更新無視システムが追加されたため)
                                # if id_diff >= 0.8 * num_of_projects_to_monitor:
                                #     print(f"[#{num_of_checks:06}: {last_check}]  ~  #{max_id} |{id_diff:3}p /{(time_diff):6.3f}s =  --.--- p/s                                                   *{num_of_projects_to_monitor:3}")
                                #
                                # else:
                                before_update_num = last_update_num

                                recent_speed_list.append(create_speed)
                                if len(recent_speed_list) > 50:
                                    recent_speed_list.pop(0)

                                # 情報を直近50更新での情報リストに追加 (統計データ用)
                                recent_update_list_for_stats.append({"time": last_update_num, "id": max_id})
                                if len(recent_update_list_for_stats) > 50:
                                    recent_update_list_for_stats.pop(0)

                                if len(recent_update_list_for_stats) > 1:
                                    # 50件前の更新時の情報から作成速度を計算
                                    create_speed_for_stats = (max_id - recent_update_list_for_stats[0]["id"]) / (last_update_num - recent_update_list_for_stats[0]["time"])

                                    avg = lambda l : sum(l) / len(l)
                                    create_speed_last50 = avg(recent_speed_list)
                                    create_speed_last10 = avg(recent_speed_list[-10:] if len(recent_speed_list) >= 10 else recent_speed_list)

                                else:
                                    create_speed_last50 = create_speed
                                    create_speed_last10 = create_speed

                                # data_string の設定
                                data_string = f"{max_id:010}{int(create_speed_last10 * 1e5):08}{int(create_speed_last50 * 1e5):08}{int(last_update_num * 1000):014}"

                                print(f"[#{num_of_checks:06}: {last_check}] [!] #{max_id} |{id_diff:3}p /{time_diff:6.3f}s = {create_speed:7.3f} p/s (10: {create_speed_last10:6.3f} p/s, 50: {create_speed_last50:6.3f} p/s, s4s: {create_speed_for_stats:6.3f} p/s) *{num_of_projects_to_monitor:3}")

                                last_update_minus_30min = last_update - timedelta(minutes=30)

                                day_week_idx = last_update_minus_30min.weekday() * 24 + last_update_minus_30min.hour

                                # 長期的な作成速度リストの更新
                                temp = long_term_speed_data[day_week_idx]

                                # temp[0] -> avg, temp[1] -> cnt
                                if temp[0] != None:
                                    temp[0] = ((temp[0] * temp[1]) + create_speed_for_stats) / (temp[1] + 1)
                                    temp[1] += 1

                                else:
                                    temp[0] = create_speed
                                    temp[1] = 1

                                long_term_speed_data[day_week_idx] = temp

                                # 長期的な作成速度文字列の更新 (自動でScratchにはアップロードしない。ファイルに入れてy_nkが手動更新)
                                speed_data_string = ""

                                for i, inner in zip(range(168), long_term_speed_data):
                                    if inner[0] != None:
                                        speed_data_string += f"{int(inner[0] * 1e8):010}"
                                    else:
                                        speed_data_string += f"{0:010}"

                                before_max_id = max_id

                        else:
                            before_max_id = max_id
                    
                    else:
                        unfound_streak += 1

                        if num_of_updates_gonna_be_ignored == 0:
                            print(f"[#{num_of_checks:06}: {last_check}]     #{max_id} |  0p /{(last_check_num - last_update_num):6.3f}s =   0     p/s                                                   *{num_of_projects_to_monitor:3}")
                        
                        else:
                            print(f"[#{num_of_checks:06}: {last_check}]  -  #{max_id} |{max_id - before_max_id:3}p /{last_check_num - last_update_num:6.3f}s = {(max_id - before_max_id) / (last_check_num - last_update_num):7.3f} p/s                                                   *{num_of_projects_to_monitor:3}")

                    num_of_checks += 1

                    # キリ番に到達した場合
                    if max_id >= next_milestone:
                        # before_milestones の中身を新しいやつで押し出す
                        before_milestones.insert(0, {"id": next_milestone, "shared": False, "creator": None, "created_time": int(last_check_num)})
                        before_milestones.pop()
                        
                        # milestone_dataやstring を更新
                        next_milestone = int((math.ceil((max_id + 1) / MILESTONE_STEP)) * MILESTONE_STEP)
                        if any([max_id < e < next_milestone for e in SPECIAL_MILESTONES]):
                            next_milestone = [e for e in SPECIAL_MILESTONES if max_id < e < next_milestone][0]

                        milestone_string = ""
                        for j in range(NUM_OF_MILESTONES_TO_MONITOR):
                            if before_milestones[j]["shared"]:
                                milestone_string += f"{before_milestones[j]['id']}1{convert_username_to_numstr(before_milestones[j]["creator"])}{int(before_milestones[j]["created_time"]):010}"
                            else:
                                milestone_string += f"{before_milestones[j]['id']}0{0:040}{int(before_milestones[j]["created_time"]) if before_milestones[j]["created_time"] != None else 0:010}"
                            
                        # 念入り
                        await set_var("milestone_data", milestone_string)
                        await set_var("milestone_data", milestone_string)
                        await set_var("milestone_data", milestone_string)

                        print(f"\n[{last_check}] <<< Milestone #{before_milestones[0]["id"]} has just been created!! (Next: #{next_milestone}) >>>\n")

                    if num_of_checks >= 30:
                        await set_var("data", data_string)

                    data = {"max_id": max_id, "before_milestones": before_milestones}

                    # データたちを各ファイルに書き込み
                    for err_cnt in range(1000000):  # エラーリトライ用
                        try:
                            with open(path.join(DIR_PATH, "data.json"), "w") as file:
                                json.dump(data, file, indent=4)

                            with open(path.join(DIR_PATH, "speed_data.csv"), "w", newline="") as file:
                                writer = csv.writer(file)
                                writer.writerows(long_term_speed_data)
                            
                            with open(path.join(DIR_PATH, "speed_data_for_copy.txt"), "w") as file:
                                file.write(speed_data_string)

                        except Exception as e:
                            if err_cnt >= 1000000: raise e

                            print(f"Error while writing data to files... count: {err_cnt}, err: {e!r}")
                            await asyncio.sleep(0.3)
                        else:
                            break
                
            except Exception: 
                print(f"[{datetime.utcnow()}] The following error occurred!\n {traceback.format_exc()}\n")

                with open(path.join(DIR_PATH, "error_dump.log"), "a", encoding='UTF-8') as file:
                    file.write(f"[{datetime.utcnow()}] The following error occurred.\n {traceback.format_exc(limit=1)}\n\n")

                await asyncio.sleep(5)

asyncio.run(main())