import pandas as pd
import requests as req
import re
import json
import xml.etree.ElementTree as elemtree
import zipfile
import pickle

# import FinanceDataReader as fdr
import numpy as np
from itertools import chain
from html_table_parser import parser_functions as parser
from bs4 import BeautifulSoup as bs
from selenium import webdriver


class DisclosureManagement:  # 다트 API를 사용해 필요한 정보들 가져오는 클래스
    def __init__(self, api_key):  # API 키는 현재 폴더에 api_key.txt에 존재
        self.api_key = api_key

    def push_rcept_no_df(
        self, corp_code, bgn_de, end_de, pblntf_ty, page_no, page_count
    ):  # 공시 정보 및 유형, 고유 번호 등의 데이터프레임 반환, YYYYMMDD형식으로 날짜 전달

        """
        pblntf_ty: 공시유형에 대한 내용
        A: 정기공시
        B: 주요사항보고
        C: 발행공시
        D: 지분공시
        E: 기타공시
        F: 외부감사관련
        G: 펀드공시
        H 자산유동화
        I: 거래소 공시
        J: 공정위 공시

        sort_mth: 정렬방법, (asc / desc) => 사용 시 에러...
        page_no: 페이지 번호, 기본값: 1
        page_count: 페이지 당 건수, 기본값: 10, 최댓값: 100
        """

        url = "https://opendart.fss.or.kr/api/list.json?crtfc_key={}&corp_code={}&bgn_de={}&end_de={}&pblntf_ty={}&page_no={}&page_count={}".format(
            self.api_key, corp_code, bgn_de, end_de, pblntf_ty, page_no, page_count
        )
        res = req.get(url)
        json_dict = json.loads(res.content)

        if json_dict["status"] == "013":
            return json_dict["status"]
        else:
            return pd.DataFrame(json_dict["list"])  # 데이터프레임 반환

    def summary_corp(self, corp_code):

        """
        status	에러 및 정보 코드
        message	에러 및 정보 메시지
        corp_name	정식명칭		정식회사명칭
        corp_name_eng	영문명칭		영문정식회사명칭
        stock_name	종목명(상장사) 또는 약식명칭(기타법인)		종목명(상장사) 또는 약식명칭(기타법인)
        stock_code	상장회사인 경우 주식의 종목코드		상장회사의 종목코드(6자리)
        ceo_nm	대표자명		대표자명
        corp_cls	법인구분		법인구분 : Y(유가), K(코스닥), N(코넥스), E(기타)
        jurir_no	법인등록번호		법인등록번호
        bizr_no	사업자등록번호		사업자등록번호
        adres	주소		주소
        hm_url	홈페이지		홈페이지
        ir_url	IR홈페이지		IR홈페이지
        phn_no	전화번호		전화번호
        fax_no	팩스번호		팩스번호
        induty_code	업종코드		업종코드
        est_dt	설립일(YYYYMMDD)		설립일(YYYYMMDD)
        acc_mt	결산월(MM)		결산월(MM)
        """

        url = "https://opendart.fss.or.kr/api/company.json?crtfc_key={}&corp_code={}".format(
            self.api_key, corp_code
        )
        res = req.get(url)
        json_dict = json.loads(res.content)

        return pd.DataFrame([json_dict]).T

    def disclosure_viewer(self, rcept_no):  # 문서번호는 push_rcept_no_df 데이터프레임에 담겨있음

        """
        문서 접수 번호를 인자로 넣어주면 그에 맞는 공시 뷰어 URL로 이동 후 브라우저 작동
        """

        viewer_url = "http://dart.fss.or.kr/dsaf001/main.do?rcpNo={}".format(rcept_no)
        chrome_webdriver = webdriver.Chrome("./chromedriver.exe")

        chrome_webdriver.get(viewer_url)

    def important_account(
        self, corp_code, bsns_year, reprt_code
    ):  # 단일 재무제표 중요 항목 데이터프레임 반환, 사업연도는 YYYY형식으로 전달

        """
        2월 중으로 DART에서 전 계정에 대한 API 제공할 예정, 그 이후에는 사용될 일 없으나 간단하게 재무상태를 비교하거나 확인해볼 때 유용

        corp_code: 8자리 기업 고유 번호

        bsns_year: 4자리 사업연도

        reprt_code: 보고서 유형
        1분기 보고서: 11013
        반기 보고서: 11012
        3분기 보고서: 11014
        사업 보고서: 11011
        """

        url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json?crtfc_key={}&corp_code={}&bsns_year={}&reprt_code={}".format(
            self.api_key, corp_code, bsns_year, reprt_code
        )
        res = req.get(url)
        json_dict = json.loads(res.content)
        important_account_df = pd.DataFrame(json_dict["list"])
        col = [
            "계정명",
            "전전기금액",
            "전전기일자",
            "전전기명",
            "고유번호",
            "전기금액",
            "전기일자",
            "전기명",
            "개별/연결구분",
            "개별/연결명",
            "계정과목 정렬순서",
            "보고서코드",
            "재무재표구분",
            "재무제표명",
            "종목코드",
            "당기금액",
            "당기일자",
            "당기명",
        ]

        important_account_df.columns = col
        return important_account_df

    def original_code_df(self, path, file_name):  # 상장 기업의 고유번호 데이터프레임 반환

        """
        공시 대상 기업의 고유번호를 파일(zip)형태로 제공, 응답방식: STREAM
        path: zip 파일을 저장하고자 하는 경로
        file_name: 파일에 지정하고자 하는 이름
        """

        url = "https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={}".format(
            self.api_key
        )

        res = req.get(url)

        with open("{}{}".format(path, file_name), "wb") as save:
            for chunk in res.iter_content(
                chunk_size=512
            ):  # 파일을 stream 해야 하고 용량이 크기 때문에 chunk 만큼 잘라서 데이터 streaming
                save.write(chunk)

        original_code_zip_file = zipfile.ZipFile("{}{}".format(path, file_name))
        original_code_zip_file.extractall("{}".format(path))
        tree = elemtree.parse(
            "{}CORPCODE.xml".format(path)
        )  # zipfile로 저장한 데이터를 압축해제 후 xml로 loading

        root = tree.getroot()  # root에서부터 xml 태그 분석

        dict_ = {}

        for code, name in zip(
            root.findall("list/corp_name"), root.findall("list/corp_code")
        ):
            dict_[code.text] = [name.text]

        return pd.DataFrame(dict_, index=["기업코드"]).T

    def get_original_document(self, rcept_no, path, file_name):

        url = "https://opendart.fss.or.kr/api/document.xml?crtfc_key={}&rcept_no={}".format(
            self.api_key, rcept_no
        )

        res = req.get(url)

        with open("{}{}".format(path, file_name), "wb") as save:
            for chunk in res.iter_content(chunk_size=512):
                save.write(chunk)


class NaverNiceCrawling:
    def __init__(self, corp_sym_name_df):
        self.corp_sym_name_df = corp_sym_name_df

    def naver_fs_crawler(self):

        for iter_ in self.corp_sym_name_df.iterrows():
            print(iter_)
            corp_code, corp_name = iter_[1]["Symbol"], iter_[1]["Name"]

            url = "http://companyinfo.stock.naver.com/v1/company/cF1001.aspx?cmp_cd={}&fin_typ=0&freq_typ=Y".format(
                corp_code
            )

            res = req.get(url)
            html_ = bs(res.content, "lxml")

            table_1 = html_.table
            table_2 = parser.make2d(table_1)

            bs_data = pd.DataFrame(table_2).T
            bs_data = bs_data.set_index(1).T.set_index("주요재무정보")
            bs_data.index.name = corp_name, corp_code

            yield bs_data.iloc[:, 2:8]  # 제너레이터 형태로 반환

    def nice_fs_crawler(self, bs_type):

        for iter_ in self.corp_sym_name_df.iterrows():
            idx, corp_code, corp_name = iter_[0], iter_[1]["Symbol"], iter_[1]["Name"]

            url = "http://media.kisline.com/highlight/mainHighlight.nice?paper_stock={}&nav=1".format(
                corp_code
            )

            res = req.get(url)
            html_ = bs(res.content, "lxml")

            table_1 = html_.findAll("table")

            if len(table_1) <= 13:
                print("{}에서 문제가 발생했습니다.".format(idx))

            else:
                if bs_type == "개별":
                    table_2_annual = pd.DataFrame(parser.make2d(table_1[5]))
                    table_2_quater = pd.DataFrame(parser.make2d(table_1[6]))

                else:
                    table_2_annual = pd.DataFrame(parser.make2d(table_1[7]))
                    table_2_quater = pd.DataFrame(parser.make2d(table_1[8]))

                bs_data = pd.concat([table_2_annual, table_2_quater], 1)
                bs_data.columns = bs_data.iloc[1, :]
                bs_data = bs_data.set_index("구분")
                bs_data.index.name = corp_name, corp_code
                bs_data.columns.name = ""
                bs_data = bs_data.drop(["", "구분"])

                yield bs_data  # 제너레이터 형태로 반환
