#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import re
import pickle
import pandas as pd
import math
import numpy as np
import random
import requests
#from bs4 import BeautifulSoup
import json
import os
from wmfdata.spark import get_session


# In[ ]:


get_ipython().system('which python')


# In[ ]:


qids_and_properties={}


# In[ ]:


# Pass in directory to place output files
output_dir = 'Output'

if not os.path.exists(output_dir):
    os.makedirs(output_dir)
    
# Pass in the full snapshot date
snapshot = '2021-07-26'

# Allow the passing of a single language as a parameter
language = 'kowiki'

# A spark session type determines the resource pool
# to initialise on yarn
spark_session_type = 'regular'

# Name of placeholder images parquet file
image_placeholders_file = 'image_placeholders'


# In[ ]:


# We use wmfdata boilerplate to init a spark session.
# Under the hood the library uses findspark to initialise
# Spark's environment. pyspark imports will be available 
# after initialisation
spark = get_session(type='regular', app_name="ImageRec-DEV Training")
import pyspark
import pyspark.sql


# In[ ]:


languages=['enwiki','arwiki','kowiki','cswiki','viwiki','frwiki','fawiki','ptwiki','ruwiki','trwiki','plwiki','hewiki','svwiki','ukwiki','huwiki','hywiki','srwiki','euwiki','arzwiki','cebwiki','dewiki','bnwiki'] #language editions to consider
#val=100 #threshold above which we consider images as non-icons

languages=[language]


# In[ ]:


reg = r'^([\w]+-[\w]+)'
short_snapshot = re.match(reg, snapshot).group()
short_snapshot


# In[ ]:


reg = r'.+?(?=wiki)'
label_lang = re.match(reg, language).group()
label_lang


# In[ ]:


get_ipython().system('ls /home/gmodena/ImageMatching/conf/metrics.properties')


# In[ ]:


len(languages)


# In[ ]:


image_placeholders = spark.read.parquet(image_placeholders_file)
image_placeholders.createOrReplaceTempView("image_placeholders")


# In[ ]:


def get_threshold(wiki_size):
    #change th to optimize precision vs recall. recommended val for accuracy = 5
    sze, th, lim = 50000, 15, 4 
    if (wiki_size >= sze):
        #if wiki_size > base size, scale threshold by (log of ws/bs) + 1
        return (math.log(wiki_size/sze, 10)+1)*th
    #else scale th down by ratio bs/ws, w min possible val of th = th/limiting val
    return max((wiki_size/sze) * th, th/lim)


# In[ ]:


val={}
total={}
for wiki in languages:
     querytot="""SELECT COUNT(*) as c
     FROM wmf_raw.mediawiki_page
     WHERE page_namespace=0 
     AND page_is_redirect=0
     AND snapshot='"""+short_snapshot+"""' 
     AND wiki_db='"""+wiki+"""'"""
     wikisize = spark.sql(querytot).toPandas()
     val[wiki]=get_threshold(int(wikisize['c']))
     total[wiki]=int(wikisize['c'])


# In[ ]:


val


# In[ ]:


total


# In[ ]:


wikisize


# The query below retrieves, for each unillustrated article: its Wikidata ID, the image of the Wikidata ID (if any), the Commons category of the Wikidata ID (if any), and the lead images of the articles in other languages (if any).
# 
# `allowed_images` contains the list of icons (images appearing in more than `val` articles)
# 
# `image_pageids` contains the list of illustrated articles (articles with images that are not icons)
# 
# `noimage_pages` contains the pageid and Qid of unillustrated articles
# 
# `qid_props` contains for each Qid in `noimage_pages`, the values of the following properties, when present:
# * P18: the item's image
# * P373: the item's Commons category
# * P31: the item's "instance of" property
# 
# `category_image_list` contains the list of all images in a Commons category in `qid_props`
# 
# `lan_page_images` contains the list of lead images in Wikipedia articles in all languages linked to each Qid
# 
# `qid_props_with_image_list` is qid_props plus the list of images in the Commons category linked to the Wikidata item
# 
# 

# In[ ]:


for wiki in languages:
     print(wiki)
     queryd="""WITH allowed_images AS 
     (
     SELECT il_to
     FROM wmf_raw.mediawiki_imagelinks
     WHERE il_from_namespace=0 
     AND snapshot='"""+short_snapshot+"""'  
     AND wiki_db='"""+wiki+"""' 
     AND il_to not like '%\"%' AND il_to not like '%,%'
     GROUP BY il_to  
     HAVING COUNT(il_to)>"""+str(val[wiki])+"""),
     image_pageids AS 
     (SELECT DISTINCT il_from as pageid
     FROM wmf_raw.mediawiki_imagelinks il1 
     LEFT ANTI JOIN allowed_images
     ON allowed_images.il_to=il1.il_to
     WHERE il1.il_from_namespace=0 
     AND il1.wiki_db='"""+wiki+"""' 
     AND il1.snapshot='"""+short_snapshot+"""'
     ),
     pageimage_pageids AS 
     (
     SELECT DISTINCT pp_page as pageid
     FROM wmf_raw.mediawiki_page_props pp
     WHERE pp.wiki_db ='"""+wiki+"""'
     AND pp.snapshot='"""+short_snapshot+"""'
     AND pp_propname in ('page_image','page_image_free')),
     all_image_pageids as(
     SELECT pageid 
     FROM image_pageids 
     UNION
     SELECT pageid
     FROM pageimage_pageids
     ),
     noimage_pages as 
     (
     SELECT wipl.item_id,p.page_id,p.page_title,page_len
     FROM wmf_raw.mediawiki_page p 
     JOIN wmf.wikidata_item_page_link wipl
     ON p.page_id=wipl.page_id
     LEFT ANTI JOIN all_image_pageids
     on all_image_pageids.pageid=wipl.page_id
     WHERE p.page_namespace=0 
     AND page_is_redirect=0 AND p.wiki_db='"""+wiki+"""' 
     AND p.snapshot='"""+short_snapshot+"""' 
     AND wipl.snapshot='"""+snapshot+"""'
     AND wipl.page_namespace=0
     AND wipl.wiki_db='"""+wiki+"""'
     ORDER BY page_len desc
     ),
     qid_props AS 
     (
     SELECT we.id,label_val, 
     MAX(CASE WHEN claim.mainSnak.property = 'P18' THEN claim.mainSnak.datavalue.value ELSE NULL END) AS hasimage,
     MAX(CASE WHEN claim.mainSnak.property = 'P373' THEN REPLACE(REPLACE(claim.mainSnak.datavalue.value,'\"',''),' ','_') ELSE NULL END) AS commonscategory,
     MAX(CASE WHEN claim.mainSnak.property = 'P31' THEN claim.mainSnak.datavalue.value ELSE NULL END) AS instanceof
     FROM wmf.wikidata_entity we
     JOIN noimage_pages
     ON we.id=noimage_pages.item_id
     LATERAL VIEW explode(labels) t AS label_lang,label_val
     LATERAL VIEW OUTER explode(claims) c AS claim
     WHERE typ='item'
     AND t.label_lang='"""+label_lang+"""'
     AND snapshot='"""+snapshot+"""'
     AND claim.mainSnak.property in ('P18','P31','P373')
     GROUP BY id,label_val
     ),
     category_image_list AS
     (
     SELECT cl_to,concat_ws(';',collect_list(mp.page_title)) as category_imagelist
     from qid_props
     left join wmf_raw.mediawiki_categorylinks mc
     on qid_props.commonscategory=mc.cl_to
     join wmf_raw.mediawiki_page mp
     on mp.page_id=mc.cl_from
     LEFT ANTI JOIN image_placeholders
     on image_placeholders.page_title = mp.page_title
     WHERE mp.wiki_db ='commonswiki'
     AND mp.snapshot='"""+short_snapshot+"""'
     AND mp.page_namespace=6
     AND mp.page_is_redirect=0
     AND mc.snapshot='"""+short_snapshot+"""'
     AND mc.wiki_db ='commonswiki'
     AND mc.cl_type='file'
     group by mc.cl_to
     ),
     qid_props_with_image_list AS
     (
     SELECT id, label_val, hasimage, commonscategory, instanceof,category_imagelist
     from qid_props
     left join category_image_list
     on qid_props.commonscategory=category_image_list.cl_to
     ),
     lan_page_images AS
     (
     SELECT nip.item_id,nip.page_id,nip.page_title,nip.page_len,collect_list(concat(pp.wiki_db,': ',pp.pp_value)) as lan_images
     FROM noimage_pages nip
     LEFT JOIN  wmf.wikidata_item_page_link wipl
     LEFT JOIN wmf_raw.mediawiki_page_props pp
     LEFT JOIN wmf_raw.mediawiki_page mp
     ON nip.item_id=wipl.item_id
     AND wipl.page_id=pp.pp_page
     AND wipl.wiki_db=pp.wiki_db
     AND mp.page_title=pp.pp_value
     LEFT ANTI JOIN image_placeholders
     ON image_placeholders.page_title = pp.pp_value
     WHERE wipl.wiki_db !='"""+wiki+"""'
     AND wipl.snapshot='"""+snapshot+"""'
     AND wipl.page_namespace=0
     AND pp.snapshot='"""+short_snapshot+"""'
     AND pp_propname in ('page_image','page_image_free')
     AND mp.wiki_db ='commonswiki'
     AND mp.snapshot='"""+short_snapshot+"""'
     AND mp.page_namespace=6
     AND mp.page_is_redirect=0
     GROUP BY nip.item_id,nip.page_id,nip.page_title,nip.page_len
     ),
     joined_lan_page_images AS
     (
     SELECT nip.item_id,nip.page_id,nip.page_title,nip.page_len, lpi.lan_images
     from noimage_pages nip
     LEFT JOIN lan_page_images lpi
     on nip.item_id=lpi.item_id
     )
     SELECT * from joined_lan_page_images
     LEFT JOIN qid_props_with_image_list
     on qid_props_with_image_list.id=joined_lan_page_images.item_id
     
     """
     qid_props = spark.sql(queryd).toPandas()
     qids_and_properties[wiki]=qid_props
 


# Below I am just creating different tables according to whether an image is retrieved from a specific source (Wikidata image, Commons Category, or interlingual links)

# In[ ]:


hasimage={}
commonscategory={}
lanimages={}
allimages={}
for wiki in languages:
    print(wiki)
    hasimage[wiki]=qids_and_properties[wiki][qids_and_properties[wiki]['hasimage'].astype(str).ne('None')]
    commonscategory[wiki]=qids_and_properties[wiki][qids_and_properties[wiki]['category_imagelist'].astype(str).ne('None')]
    lanimages[wiki]=qids_and_properties[wiki][qids_and_properties[wiki]['lan_images'].astype(str).ne('None')]
    print("number of unillustrated articles: "+str(len(qids_and_properties[wiki])))
    print("number of articles items with Wikidata image: "+str(len(hasimage[wiki])))
    print("number of articles items with Wikidata Commons Category: "+str(len(commonscategory[wiki])))
    print("number of articles items with Language Links: "+str(len(lanimages[wiki])))
    ####
    allimages[wiki]=qids_and_properties[wiki]


# Below the two functions to select images depending on the source:
# * `select_image_language` takes as input the list of images from articles in multiple languages and selects the one which is used more often across languages (after some major filtering)
# * `select_image_category` selects at random one of the images in the Commons category linked to the Wikidata item.

# In[ ]:


def image_language_checks(iname):
    #list of substrings to check for
    substring_list=['.svg','flag','noantimage','no_free_image','image_manquante',
                    'replace_this_image','disambig','regions','map','map','default',
                    'defaut','falta_imagem_','imageNA','noimage','noenzyimage']
    iname=iname.lower()
    if any(map(iname.__contains__, substring_list)):
        return False
    else:
        return True

def select_image_language(imagelist): 
    counts={} #contains counts of image occurrences across languages
    languages={} #constains which languages cover a given image
    #for each image
    for image in imagelist:
        data=image.strip().split(' ')#this contains the language and image name data
        ###
        if len(data)==2: #if we actually have 2 fields
            iname=data[1].strip()
            lan=data[0].strip()[:-1]
            ###
            if iname not in counts: #if this image does not exist in our counts yet, initialize counts
                if not image_language_checks(iname): #if the image name is not valid
                    continue
               # urll = 'https://commons.wikimedia.org/wiki/File:'+iname.replace(' ','_')+'?uselang='+language
                #page = requests.get(urll)
                #if page.status_code == 404:
                 #   print (urll)
                 #   continue
                counts[iname]=1
                languages[iname]=[]
            else:
                counts[iname]+=1
            languages[iname].append(lan)
    return languages

def select_image_category(imagelist):
    counts={}
    languages={}
    data=list(imagelist.strip().split(';'))
    data=[d for d in data if d.find('.')!=-1]
    return random.choice(data)


# Below the priority assignment process:
# * If the article has a Wikidata image (not a flag, as this is likely a duplicate), give it priority 1
# * Choose up to 3 images among the ones from related Wikipedia articles  in other languages, using the `select_image_language` function, and give priority 2.x where `x` is a ranking given by the number of languages using that image 
# * If the article has an associated Commons category, call the `select_image_category` function, randomly selecting up to 3 images form that category
# 

# In[ ]:



stats={}
data_small={}
####
for wiki in languages:
    selected=[] #stores selected images for each article
    notes=[] #stores information about the source where the candidate image was drawn from
    wikis=[]
    data_small[wiki]=allimages[wiki].sample(len(allimages[wiki]))
    language=wiki.replace('wiki','')
    #rtl=direction[wiki] #right to left -> rtl; left to right -> ltr
    for wikipedia in data_small[wiki]['lan_images']:
        if str(wikipedia)!='None':
                lg=select_image_language(wikipedia)
                if len(lg)==0:
                    lg=None
                wikis.append(lg)
        else:
            wikis.append(None)
    data_small[wiki]['wikipedia_imagelist']=wikis
    for wikidata,commons,wikipedia,jdata in zip(data_small[wiki]['hasimage'],data_small[wiki]['category_imagelist'],data_small[wiki]['wikipedia_imagelist'],data_small[wiki]['instanceof']):
        if jdata is not None:
            qid=json.loads(jdata)["numeric-id"]
            if qid in [4167410,577,13406463]:
                selected.append(None)
                notes.append(None)
                continue
        image=None
        tier={}
        note={}
        if str(commons)!='None':
            for i in range(min(len(list(commons.strip().split(';'))),3)):
                image=select_image_category(commons)
                tier[image]=3
                note[image]='image was found in the Commons category linked in the Wikidata item'
            ###
        if str(wikipedia) !='None':
            index=np.argsort([len(l) for l in list(wikipedia.values())])
            #print(wikipedia)
            for i in range(min(len(wikipedia),3)):
                image=list(wikipedia.keys())[index[-(i+1)]]
                tier[image]=2+(float(i)/10)
                note[image]='image was found in the following Wikis: '+', '.join(wikipedia[image])
        if str(wikidata)!='None' and wikidata.lower().find('flag') ==-1:
            image=wikidata[1:-1]
            tier[image]=1
            note[image]='image was in the Wikidata item'
        selected.append(tier if len(tier)>0 else None)
        notes.append(note  if len(note)>0 else None)
#         if image is not None:
#             properties.append(get_properties(image,language,rtl,page))
#         else:
#             properties.append([None,None,None,None,None,None,None,None,None])
    #updating table
    data_small[wiki]['selected']=selected
    data_small[wiki]['notes']=notes
    data_small[wiki]['good_interlinks']=wikis
    #TODO(REMOVE FROM repo) data_small[wiki]=data_small[wiki][data_small[wiki]['selected'].astype(str).ne('None')]
    #print("total number of articles: "+str(total[wiki]))
    #print("number of unillustrated articles: "+str(len(qids_and_properties[wiki])))
    #print("number of articles with at least 1 recommendation: "+str(len(data_small[wiki])))
    #stats[wiki]=[total[wiki],len(qids_and_properties[wiki]),len(data_small[wiki]),len(all3images),len(hasimage),len(commonscategory),len(lanimages)]


# In[ ]:


#the final selection process: select up to 3 images per candidateand their relative confidence score (1=high, 2=medium, 3=low) 
#based on the priorities assigned earlier
for wiki in languages:
    top_candidates=[]
    for selected,notes in zip (data_small[wiki]['selected'],data_small[wiki]['notes']):
        if selected is not None:
            index=np.argsort([l for l in list(selected.values())])
            candidates=[]
            #print(wikipedia)
            for i in range(min(len(index),3)):
                    image=list(selected.keys())[index[i]]
                    rating=selected[image]
                    note=notes[image]
                    candidates.append({'image':image,'rating':rating,'note':note})
            top_candidates.append(candidates)
        else:
            top_candidates.append(None)
    data_small[wiki]['top_candidates']=top_candidates
    data_small[wiki][['item_id','page_id','page_title','top_candidates', 'instanceof']].to_csv(output_dir+'/'+wiki+'_'+snapshot+'_wd_image_candidates.tsv',sep='\t')


# In[ ]:


spark.stop()

