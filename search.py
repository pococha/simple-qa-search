import sys
import os
import collections
import re

from tqdm import tqdm
import pandas as pd
import torch
import numpy as np
from numpy import dot
from numpy.linalg import norm
from transformers import BertTokenizer, BertModel


# model = BertModel.from_pretrained('hfl/chinese-roberta-wwm-ext')
# tokenizer = BertTokenizer.from_pretrained('hfl/chinese-roberta-wwm-ext')
# model = BertModel.from_pretrained('hfl/chinese-roberta-wwm-ext-large')
# tokenizer = BertTokenizer.from_pretrained('hfl/chinese-roberta-wwm-ext-large')


def remove_punc(lines):
    ''' remove text or list of string punctuations'''
    PUNC_REGEX = "！？｡。＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃》「」『』【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏."
    if type(lines) == str:
        ret = re.sub(r"[%s]+" %PUNC_REGEX, '', lines)
        return ret
    
    ret = []
    for line in lines:
        temp = re.sub(r"[%s]+" %PUNC_REGEX, '', line)
        ret.append(temp)
    return ret

def read_ques_anws_from(df, kb_name):
    '''read formatted data from dataframe'''
    ret = []
    for ind in df.index:
        cur_anw = df.iloc[ind, 4].strip()
        cur_std_ques = df.iloc[ind, 2]
        cur_add_ques = df.iloc[ind, 3].split('\n')
        cur_all_ques = [cur_std_ques] + cur_add_ques
        cur_all_raw_ques = [x.strip() for x in cur_all_ques]
        cur_all_ques = [remove_punc(x) for x in cur_all_raw_ques if x]
        for i, cur_ques in enumerate(cur_all_ques):
            cur_dict = {}
            cur_dict['raw_question'] = cur_all_raw_ques[i]
            cur_dict['question'] = cur_ques
            cur_dict['answer'] = cur_anw
            cur_dict['question_set_index'] = ind
            cur_dict['kb_name'] = kb_name
            ret.append(cur_dict)
    return ret

def dict_add_vectors(dic_list, model, tokenizer):
    ''' calculate and add vectors to list of dictionaries'''
    dic_list = dic_list.copy()
    for dic in tqdm(dic_list):
        dic['question'] = remove_punc(dic['question'].strip())
        text = dic['question']
        inputs = tokenizer(text, return_tensors = 'pt')
        outputs = model(**inputs)
        vector = outputs.last_hidden_state[0][0]
#         vector = outputs.pooler_output[0]
        dic['vector'] = vector.detach().numpy()
    return dic_list

def get_question_set(dic_list):
    ''' return dictionary of question set mappings'''
    ret = {}
    for dic in dic_list:
        if dic['question_set_index'] in ret:
            ret[dic['question_set_index']].append(dic['raw_question'])
        else:
            ret[dic['question_set_index']] = [dic['raw_question']]
    return ret

def search_query(query, dic_list, model, tokenizer):
    '''searching one query and return top 5 results according to vector similarities'''
    
    question_sets = get_question_set(dic_list) # get quesiton set mapping
    query = remove_punc(query).strip() # remove punctuation and white space
    inputs = tokenizer(query, return_tensors = 'pt') # model input
    outputs = model(**inputs) # model output
    query_vec = outputs.last_hidden_state[0][0].detach().numpy() # [CLS] vector
#     query_vec = outputs.pooler_output[0].detach().numpy()
    
    for dic in tqdm(dic_list): # calculate vector similarity distances of query vector and kb questions
        dic_vec = dic['vector']
#         print(len(query_vec))
#         print(len(dic_vec))
        cur_distance = float(norm(dic_vec - query_vec))
        dic['distance'] = cur_distance
        cur_cos_dis = float(dot(dic_vec, query_vec)/(norm(query_vec)*norm(dic_vec)))
        dic['cos_distance'] = cur_cos_dis
    cur_dic_list = sorted(dic_list, key = lambda x: x['cos_distance'],reverse = True) # sorting according to cos distance
    ret = []
    
    for dic in cur_dic_list: # return structure (without vector)
        cur_dic = {}
        cur_dic['raw_question'] = dic['raw_question']
        cur_dic['question'] = dic['question']
        cur_dic['question_set'] = question_sets[dic['question_set_index']]
        cur_dic['distance'] = dic['distance']
        cur_dic['question_set_index'] = dic['question_set_index']
        cur_dic['cos_distance'] = dic['cos_distance']
        cur_dic['answer'] = dic['answer']
        ret.append(cur_dic)
    return ret[:5]



if __name__ == '__main__':
    dic_list = read_ques_anws_from('../data/kindle-完善数据.xlsx')
    dic_list_with_vec = dict_add_vectors(dic_list, model, tokenizer)
    query = '包邮'
    output = search_query(query, dic_list_with_vec, model, tokenizer)
    print(output)
