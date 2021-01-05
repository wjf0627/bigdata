import json

import requests


# 登录azkaban
# username: 登录用户名
# password: 登录密码
def Login(username, password):
    str_url = "https://dataplatform.mobvista.com:8443"
    # 登录信息
    postdata = {'username': username, 'password': password}

    # 登录，通过verify=False关闭安全验证
    login_url = str_url + '?action=login'
    r = requests.post(login_url, postdata, verify=False).json()
    return r.get('session.id')

    response = "{'flowParam': {'ScheduleTime': '2020-12-08 18:00:00'}, 'failureAction': 'finishCurrent', 'failureDingDings': [], 'notifyFailureFirst': true, 'pipelineExecution': '', 'failureDingDingsOverride': false, 'queueLevel': 0, 'nodeStatus': {'job_1': 'SUCCEEDED', 'job_3': 'CANCELLED', 'job_2': 'KILLED'}, 'pipelineLevel': '', 'successEmailsOverride': False, 'notifyFailureLast': False, 'failureEmails': [], 'disabled': [], 'concurrentOptions': 'ignore', 'successEmails': [], 'failureEmailsOverride': False}".replace(
        "'", "\"")

    j = json.loads(response.lower())
    nodestatus = j.get('nodestatus')
    success_list = []
    for key, value in nodestatus.items():
        if value == 'succeeded':
            success_list.append(key)
    print(str(success_list).replace("'", "\""))
    data = {
        "project": "test"
    }
    if success_list.__len__() > 0:
        data["disabled"] = success_list

    print(data)
    print(json.dumps(data))
    line = 'https://dataplatform.mobvista.com:8443,test_azkaban,job_3,20201209110000,8725990'
    name = 'https://dataplatform.mobvista.com:8443,test_azkaban,job_3,20201209110000'
    print(name in line)

    path = '/opt/airflow/data/20201209.txt'
    failed_execid = '0'
    with open(path, 'r') as r:
        lines = r.readlines()
    with open(path, 'w') as f_w:
        for line in lines:
            if name in line:
                failed_execid = line.split(',')[-1]
            else:
                f_w.write(line)
    print(failed_execid)
    dats = """{'session.id': '8490b9c8-56d5-4162-89f5-387511ea641f', 'ajax': 'executeFlow', 'project': 'test_azkaban', 'flow': 'job_3', 'flowOverride[ScheduleTime]': '2020-12-09 11:00:00'}""" \
        .replace("'", "\"")

    print(json.dumps(json.loads(dats)))
    return r


def test(endpoint, data, request_type):
    response = None
    requests.packages.urllib3.disable_warnings()
    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'ALL'

    import http.client
    conn = http.client.HTTPConnection('dataplatform.mobvista.com', 8443)
    headers = {"Content-type": "application/json", "Accept": "*/*"}
    print('data %s', data)
    if request_type == 'get':
        response = conn.request('GET', 'https://dataplatform.mobvista.com:8443/executor', data, headers)
    elif request_type == 'post':
        response = conn.request('POST', 'https://dataplatform.mobvista.com:8443/executor', data, headers)
    return response


if __name__ == '__main__':
    # sessionId = Login("dataplatform_airflow@mintegral.com", "Airflow_2020")

    data = """{'session.id': 'b7978d38-6d78-4d71-92bc-567772ff52a0', 'ajax': 'executeFlow', 'project': 'test_azkaban', 'flow': 'job_3', 'flowOverride[ScheduleTime]': '2020-12-09 11:00:00', 'disabled': ['job_1', 'job_2']}""" \
        .replace("'", "\"")
    execute_endpoint = 'https://dataplatform.mobvista.com:8443/executor?project=test_azkaban&ajax=executeFlow&flow=job_3&disabled=["job_1"]&session.id=b7978d38-6d78-4d71-92bc-567772ff52a0&flowOverride[ScheduleTime]=2020-12-09 11:00:00'
    execute_endpoint = 'https://dataplatform.mobvista.com:8443/executor'
    # data = """{}"""
    execute_data = json.loads(data)
    print(execute_data)
    # response = test(execute_endpoint, json.dumps(execute_data), "get")
    # print(response)
    execution_data = ("session.id=%s&ajax=%s&project=%s&flow=%s&flowOverride[ScheduleTime]=%s" %
                      ('b7978d38-6d78-4d71-92bc-567772ff52a0', 'executeFlow', 'test_azkaban', 'job_3',
                       '2020-12-09 11:00:00'))
    disabled = ['job_1', 'job_2']
    execution_data = execution_data + ('disabled=%s' % json.dumps(disabled))
    print(execution_data)
