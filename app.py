import json
from datetime import datetime
import logging
import traceback
import configparser

from gevent.pywsgi import WSGIServer
import pandas as pd
import torch
from transformers import BertTokenizer, BertModel
# from transformers import ElectraModel, ElectraTokenizer
from flask import Flask, jsonify, request
from flask_cors import CORS

from search import *
from db_operate import DbProcess
import logger

# load model
model = BertModel.from_pretrained('hfl/chinese-bert-wwm')
tokenizer = BertTokenizer.from_pretrained('hfl/chinese-bert-wwm')
# model = BertModel.from_pretrained('hfl/chinese-bert-wwm-ext')
# tokenizer = BertTokenizer.from_pretrained('hfl/chinese-bert-wwm-ext')
# model = BertModel.from_pretrained('hfl/chinese-roberta-wwm-ext')
# tokenizer = BertTokenizer.from_pretrained('hfl/chinese-roberta-wwm-ext')
# model = BertModel.from_pretrained('hfl/chinese-roberta-wwm-ext-large')
# tokenizer = BertTokenizer.from_pretrained('hfl/chinese-roberta-wwm-ext-large')
# model = ElectraModel.from_pretrained('hfl/chinese-electra-180g-small-discriminator')
# tokenizer = ElectraTokenizer.from_pretrained('hfl/chinese-electra-180g-small-discriminator')

#load config
cf = configparser.ConfigParser()
cf.read('./config.ini')

# read data
NP_PATH = './np_array'
# DATA_PATH = './data/kindle-完善数据.xlsx'
# dic_list = read_ques_anws_from(DATA_PATH)
# dic_list_with_vec = dict_add_vectors(dic_list, model, tokenizer)

#log path
query_logger = logger.getLoggers('queryLog', logging.INFO, cf.get('log','log_query_path'))
update_logger = logger.getLoggers('uploadLog',logging.INFO, cf.get('log','log_upload_path'))

# db class
# 测试 DB
db = DbProcess(db_config = ('47.93.81.67','root','App0926Magic!','ailive_faq'),
                   np_path = NP_PATH,
                  db_name = 'qa_db_rm')
# 正式 DB
# db = DbProcess(db_config = ('rm-2ze4r9e02kp961m5hno.mysql.rds.aliyuncs.com', 'root',
#                           'Magics1213Corp!', 'mr'),
#                            np_path = NP_PATH,
#                            db_name = 'qa_db')

print('config completed')

app = Flask(__name__)
CORS(app)


# query matching
@app.route("/qa_search/v1/query",methods=['POST'])
def index():
    time_begin = datetime.now()
    try:
        content = request.get_data()
        content = json.loads(request.data)
        query = content['query']
        
            
        assert query
        kb_names = content['kb_names'] # from which names
        dic_list_with_vec = db.get_data(kb_names)
#         dic_list_with_vec = read_data_from_db(kb_names, NP_PATH)

        
        output = search_query(query, dic_list_with_vec, model, tokenizer)
        time_end = datetime.now()
        
        #log info
        line = f'SUCCESS\tstart:\t{time_begin}\ttimeconsume:\t{time_end-time_begin}\n{output}'
        query_logger.info(line)
        
        return jsonify(output)
   
    except Exception as e:
        time_end = datetime.now()
        msg = str(traceback.format_exc())
        line = 'ERROR!!!\tstart:\t'+str(time_begin)+'\ttimeconsume:\t'+str(time_end-time_begin)+'\t'+msg
        print(line)
        query_logger.info(line)
        res = jsonify({'error':'error'})
        return res
    
# update date in mysql
@app.route("/qa_search/v1/upload",methods=['POST'])
def upload():
    time_begin = datetime.now()
    res = []
    cur = {}
    try:
        # get params
        if 'file' not in request.files:
            print('no file received')
        filename = request.files['file'].filename
        file = request.files['file'].read()
        kb_name = request.form.get('kb_name')
        
        
        df = pd.read_excel(file)
        dic_list = read_ques_anws_from(df, kb_name)
        dic_list_with_vec = dict_add_vectors(dic_list, model, tokenizer) 
        
        updates = db.add_data(dic_list_with_vec, 0)
#         updates = upload_data(kb_name, dic_list_with_vec, NP_PATH)
        
        # log info
        time_end = datetime.now()
        line = f'start:\t{time_begin}\ttimeconsume:\t{time_end-time_begin}\t'
        update_logger.info(line)
        line = f'index:\t{kb_name}\tfilename:\t{filename}\tnumber of question updated:\t{updates}'
        update_logger.info(line)
        update_logger.info(json.dumps(res))
        
        # response
        cur['kb_names'] = [kb_name]
        cur['updates'] = updates
        cur['result'] = 'Success'
        res.append(cur)
        return jsonify(res)
    except Exception as e:
        time_end = datetime.now()
        msg = str(traceback.format_exc())
        line = 'ERROR!!!\tstart:\t'+str(time_begin)+'\ttimeconsume:\t'+str(time_end-time_begin)+'\t'+msg
        print(line)
        update_logger.info(line)
        return jsonify({'error':'error'})

@app.route("/qa_search/v1/empty_kbs",methods=['POST'])
def empty_kbs():
    time_begin = datetime.now()
    res = []
    cur = {}
    try:
        content = request.get_data()
        content = json.loads(request.data)
        kb_names = content['kb_names']
        
        
        updates = db.empty_data(kb_names)
#         updates = empty_kb_questions(kb_names, NP_PATH) # list of number of questions in kb_names been deleted
        
        # log info
#         time_end = datetime.now()
#         line = f'start:\t{time_begin}\ttimeconsume:\t{time_end-time_begin}\t'
#         update_logger.info(line)
#         line = f'index:\t{kb_name}\tfilename:\t{filename}\tnumber of question updated:\t{n}'
#         update_logger.info(line)
#         update_logger.info(json.dumps(res))
        
        # response
        cur['kb_names'] = kb_names
        cur['updates'] = updates
        cur['result'] = 'Success'
        res.append(cur)
        return jsonify(res)
    except Exception as e:
        time_end = datetime.now()
        msg = str(traceback.format_exc())
        line = 'ERROR!!!\tstart:\t'+str(time_begin)+'\ttimeconsume:\t'+str(time_end-time_begin)+'\t'+msg
        print(line)
        update_logger.info(line)
        return jsonify({'error':'error'})
    
    
@app.route("/qa_search/v1/add_questions",methods=['POST'])
def add_questions():
    time_begin = datetime.now()
    res = []
    cur = {}
    try:
        # get params
        content = request.get_data()
        dic_list = json.loads(request.data)
        
        
#         dic_list = content['queries']
        for dic in dic_list:
            dic['source'] = 1
            dic['raw_question'] = dic['question']
            dic['answer'] = db.get_question_answer_by(dic['kb_name'], dic['question_set_index'])
            
        dic_list_with_vec = dict_add_vectors(dic_list, model, tokenizer)
        
        updates = db.add_data(dic_list_with_vec, source = 1)
#         updates = add_questions_to_question_set(dic_list_with_vec, NP_PATH)
        
#         # log info
#         time_end = datetime.now()
#         line = f'start:\t{time_begin}\ttimeconsume:\t{time_end-time_begin}\t'
#         update_logger.info(line)
#         line = f'index:\t{kb_name}\tfilename:\t{filename}\tnumber of question updated:\t{n}'
#         update_logger.info(line)
#         update_logger.info(json.dumps(res))
        
        # response
        cur['updates'] = updates
        cur['result'] = 'Success'
        res.append(cur)
        return jsonify(res)
    except Exception as e:
        time_end = datetime.now()
        msg = str(traceback.format_exc())
        line = 'ERROR!!!\tstart:\t'+str(time_begin)+'\ttimeconsume:\t'+str(time_end-time_begin)+'\t'+msg
        print(line)
        update_logger.info(line)
        return jsonify({'error':'error'})

@app.route("/qa_search/v1/delete_questions",methods=['POST'])
def delete_questions():
    time_begin = datetime.now()
    res = []
    cur = {}
    try:
        # get params
        content = request.get_data()
        dic_list = json.loads(request.data)
        
        updates = db.delete_data(dic_list)
#         updates = delete_db_records_by(dic_list, NP_PATH)
        
#         # log info
#         time_end = datetime.now()
#         line = f'start:\t{time_begin}\ttimeconsume:\t{time_end-time_begin}\t'
#         update_logger.info(line)
#         line = f'index:\t{kb_name}\tfilename:\t{filename}\tnumber of question updated:\t{n}'
#         update_logger.info(line)
#         update_logger.info(json.dumps(res))
        
        # response
        cur['updates'] = updates
        cur['result'] = 'Success'
        res.append(cur)
        return jsonify(res)
    except Exception as e:
        time_end = datetime.now()
        msg = str(traceback.format_exc())
        line = 'ERROR!!!\tstart:\t'+str(time_begin)+'\ttimeconsume:\t'+str(time_end-time_begin)+'\t'+msg
        print(line)
        update_logger.info(line)
        return jsonify({'error':'error'})
    

    
# def get_db(content):
#     db = DbProcess(db_config = ('47.93.81.67','root','App0926Magic!','mr'),
#                    np_path = NP_PATH,
#                   db_name = 'qa_db_rm')
    
#     try:
#         temp = content['db']
#         if temp == 1:
#             db = DbProcess(db_config = ('rm-2ze4r9e02kp961m5hno.mysql.rds.aliyuncs.com', 'root',
#                           'Magics1213Corp!', 'mr'),
#                            np_path = NP_PATH,
#                            db_name = 'qa_db')
#     except:
#         pass
    
#     finally:
#         return db
    
if __name__=='__main__':
    WSGIServer(('0.0.0.0',9113),app).serve_forever()
