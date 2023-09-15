import requests as req
from bs4 import BeautifulSoup as bs
from datetime import datetime
import csv
import re,threading,pytz
file_lock = threading.Lock()

def dateFormat():
  utc_date = datetime.now(pytz.utc)
  jkt_timezone = pytz.timezone('Asia/Jakarta')
  jkt = utc_date.astimezone(jkt_timezone)
  return jkt.strftime('%d-%m-%Y')
bulan_indo = {
    "Januari": "01",
    "Februari": "02",
    "Maret": "03",
    "April": "04",
    "Mei": "05",
    "Juni": "06",
    "Juli": "07",
    "Agustus": "08",
    "September": "09",
    "Oktober": "10",
    "November": "11",
    "Desember": "12"
}
bulan_eng = {
    "January": "01",
    "February": "02",
    "March": "03",
    "April": "04",
    "May": "05",
    "June": "06",
    "July": "07",
    "August": "08",
    "September": "09",
    "October": "10",
    "November": "11",
    "December": "12"
}

def scrappers(baseUrl,tag,classtag):
  ge = req.get(baseUrl).text
  sop = bs(ge,'lxml')
  ListNews = sop.find_all(tag,class_=classtag)
  return ListNews

def get_isi(baseUrl,tag,classtag):
  ge = req.get(baseUrl).text
  sop = bs(ge,'lxml')
  isi = sop.find(tag,class_=classtag)
  return isi

def formatTanggal(tgl,bulan_):
    tanggal, bulan, tahun = tgl.split('/')
    bulan_format = bulan_.get(bulan.capitalize(), "")
    tanggal_format = f"{tanggal}-{bulan_format}-{tahun}"
    return tanggal_format

class inputCsv:
  def __init__(self,pathFile):
    self.pathFile = pathFile
    self.file_lock = threading.Lock()
  def inputHeader(self):
    with open(f'{self.pathFile}.csv', 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['judul','link','tanggal','gambar','isi','portal'])
  def inputRow(self, data):
    with self.file_lock:
      with open(f'{self.pathFile}.csv', 'a', newline='', encoding='utf-8') as file:
          writer = csv.writer(file)
          writer.writerow(data)


def itr(types,path_files):
  print('[Process] Scraping International Tax Review ..')
  date_now = dateFormat()
  if types == 'daily':
    pages = 1
  else:
    pages = 18
  urls = "https://www.internationaltaxreview.com/search?q=NEWS&f0=%3A&f0From=&f0To=&f2=00000181-1f65-d5f4-addf-9fe76fdd001b&s=1"
  for i in range(1,pages+1):
    url = f"{urls}&p={i}"
    reqUrl = req.get(url).text
    soups = bs(reqUrl,'lxml')
    ListNews = soups.find_all('ul','ListTextDesc-items')
    for x in ListNews:
      promContent = x.find_all('div',class_='PromoM-content')
      for news in promContent:
        link = news.find('a')['href']
        cookies = {
            'AWSALB': '8PUQ6zZdh3E/EfdBRVYlnIwrHicbin+s166lJYW9S3DprmvXG0OFm1LTwoNzohmi5sAE+ZS06tQLw709oYEmktgVpVqMmSW4Rtb6fSjjFFMPeIBnzzWheo9D1IuX',
            'AWSALBCORS': '8PUQ6zZdh3E/EfdBRVYlnIwrHicbin+s166lJYW9S3DprmvXG0OFm1LTwoNzohmi5sAE+ZS06tQLw709oYEmktgVpVqMmSW4Rtb6fSjjFFMPeIBnzzWheo9D1IuX',
            'blaize_session': 'b8082ba3-2c95-4da1-9fbe-5a1036123f81'}
        log = req.get(link,cookies=cookies).text
        soupLog = bs(log,'lxml')
        content = soupLog.find_all('article',class_='ArticlePage-mainContent')
        for cont in content:
          try:
            tgl = cont.find('div','ArticlePage-datePublished').text.strip().split(' ')
            tgl = f"{tgl[1].replace(',','')}/{tgl[0]}/{tgl[2]}"
            tgl = formatTanggal(tgl,bulan_eng)
            tahun = tgl.split('-')[2]
            if types == 'daily' and tgl == date_now:
                judul = cont.find('h1','ArticlePage-headline').text.strip()
                portal = 'internationaltaxreview'
                isi = cont.find('div',class_='ArticlePage-articleBody').text.strip().replace('\n','')
                try:image = cont.find('figure',class_='Figure').find('img')['src']
                except:image =''
                inputs = inputCsv(f'{path_files}{date_now}')
                inputs.inputRow([judul,link,tgl,image,isi,portal])
            elif types == 'all' and int(tahun) >= 2023:
                judul = cont.find('h1','ArticlePage-headline').text.strip()
                portal = 'internationaltaxreview'
                isi = cont.find('div',class_='ArticlePage-articleBody').text.strip().replace('\n','')
                try:image = cont.find('figure',class_='Figure').find('img')['src']
                except:image =''
                inputs = inputCsv(path_files)
                inputs.inputRow([judul,link,tgl,image,isi,portal])
            else:
              break
          except: pass
  print('[DONE] International Tax Review has been processed')
  


def ddtc(types,path_files):
  print('[Process] Scraping DDTC ..')
  date_now = dateFormat()
  stopper = True
  page = 1
  while stopper:
    url = f"https://news.ddtc.co.id/indeks/nasional?page={page}"
    soupers = scrappers(url,'div','col-span-2 row-span-2 mt-3')[0]
    classes = 'rounded-xl mt-4 bg-[#E8EFF5] dark:bg-[#262d35]'
    classes_first = 'rounded-xl mt-10 bg-[#E8EFF5] dark:bg-[#262d35]'
    p_first = soupers.find_all('div',classes_first)
    p = soupers.find_all('div',classes)
    if len(p_first) == 0 or len(p) == 0:
      stopper = False
    p.insert(0,p_first[0])
    for x in p:
      link = x.find('a')['href']
      try:
        image = x.find('div','index-images').find('img')['src']
      except:image=''
      tgls = x.find('span','text-[13px] text-[#999999] font-normal').text.strip().split(' ')[1:4]
      tgl = '/'.join(tgls)
      tgl = formatTanggal(tgl,bulan_indo)
      tahun = tgl.split('-')[2]
      if types == 'daily' and tgl == date_now:
        cont = get_isi(link,'div','col-span-2 row-span-2 mt-3')
        judul = cont.find('h1',class_='text-4xl').text.strip()
        isin = cont.find('div','fullArticle text-md')
        isi = re.sub(r'^[^–]+– ', '', ' '.join([x.text for x in isin.find_all('p')]))
        portal = 'ddtc'
        inputs = inputCsv(f'{path_files}{date_now}')
        inputs.inputRow([judul,link,tgl,image,isi,portal])
      
      elif types == 'all' and int(tahun) >= 2023:      
        cont = get_isi(link,'div','col-span-2 row-span-2 mt-3')
        judul = cont.find('h1',class_='text-4xl').text.strip()
        isin = cont.find('div','fullArticle text-md')
        isi = re.sub(r'^[^–]+– ', '', ' '.join([x.text for x in isin.find_all('p')]))
        portal = 'ddtc'
        inputs = inputCsv(path_files)
        inputs.inputRow([judul,link,tgl,image,isi,portal])
      else:
        stopper = False
        break
    page+=1
  print('[DONE] DDTC has been processed')
  


def ikpi(types,path_files):
  print('[Process] Scraping Ikpi ..')
  date_now = dateFormat()
  stopper = True
  page = 1
  while stopper:
    url = f"https://ikpi.or.id/berita/page/{page}/"
    soupers = scrappers(url,'div','bdt-post-grid-item bdt-transition-toggle bdt-position-relative')
    if len(soupers) == 0:
      stopper = False
    for x in soupers:
      link = x.find('div',class_='bdt-post-grid-img-wrap bdt-overflow-hidden').find('a')['href']
      tgl = x.find('span','bdt-post-grid-date').text.strip().replace('/','-')
      tahun = tgl.split('-')[2]
      if types == 'daily' and tgl == date_now:
        cont = get_isi(link,'div','elementor-section-wrap')
        judul = cont.find('h2',class_='elementor-heading-title elementor-size-default').text.strip()
        try:
          image = cont.find('figure',class_='wp-caption').find('img')['src']
        except:
          image = ''
        isin = cont.find('div','elementor-element elementor-element-39e1704 elementor-widget elementor-widget-theme-post-content')
        isi = re.sub(r'^[^:]+: ', '', ' '.join([x.text for x in isin.find_all('p')]))
        portal = 'ikpi'
        inputs = inputCsv(f'{path_files}{date_now}')
        inputs.inputRow([judul,link,tgl,image,isi,portal])
      elif types== 'all' and int(tahun) >= 2023:
        cont = get_isi(link,'div','elementor-section-wrap')
        judul = cont.find('h2',class_='elementor-heading-title elementor-size-default').text.strip()
        try:
          image = cont.find('figure',class_='wp-caption').find('img')['src']
        except:
          image = ''
        isin = cont.find('div','elementor-element elementor-element-39e1704 elementor-widget elementor-widget-theme-post-content')
        isi = re.sub(r'^[^:]+: ', '', ' '.join([x.text for x in isin.find_all('p')]))
        portal = 'ikpi'
        inputs = inputCsv(path_files)
        inputs.inputRow([judul,link,tgl,image,isi,portal])
      else:
        stopper = False
        break
    page+=1
  print('[DONE] Ikpi has been processed')


def hukumonline(types,path_files):
  print('[Process] Scraping HukumOnline ..')
  date_now = dateFormat()
  stopper = True
  page = 1
  while stopper:
    url = f"https://www.hukumonline.com/tag/berita/hukum-pajak?page={page}&per_page=8&sort_by=latest&profile=berita"
    soupers = scrappers(url,'div','item-news border-bottom')
    if len(soupers) < 5:
      stopper = False
    for x in soupers:
      link = "https://www.hukumonline.com"+x.find('div',class_='d-flex flex-column pl-md-4 pl-3').find('a')['href']
      tgl = x.find('span','small').text.strip()
      cont = get_isi(link,'div','col-12 col-md-8')
      juduls = cont.find('h1',class_='title-large text-main d-block pt-2 pb-1')
      if juduls != None:
        tgl = tgl.replace(' ','/')
        tgl = formatTanggal(tgl,bulan_indo)
        tgl = datetime.strptime(tgl, '%d-%m-%Y').strftime('%d-%m-%Y')
        tahun = tgl.split('-')[2]
        if types == 'daily' and tgl == date_now:
          judul = juduls.text.strip()
          try:
            image = cont.find('div','article-thumbnail-placeholder mt-md-2').find('img')['src']
          except:image=''
          isin = cont.find('article')
          isi = ' '.join([x.text for x in isin.find_all('p')])
          portal ='hukumonline'
          inputs = inputCsv(f'{path_files}{date_now}')
          inputs.inputRow([judul,link,tgl,image,isi,portal])
        elif types =='all' and int(tahun) >= 2023:
            judul = juduls.text.strip()
            try:
              image = cont.find('div','article-thumbnail-placeholder mt-md-2').find('img')['src']
            except:image=''
            isin = cont.find('article')
            portal ='hukumonline'
            isi = ' '.join([x.text for x in isin.find_all('p')])
            inputs = inputCsv(path_files)
            inputs.inputRow([judul,link,tgl,image,isi,portal])
        else:break 
    page+=1
  print('[DONE] HukumOnline has been processed')
  


def ibfd(types,path_files):
  print('[Process] Scraping IBFD ..')
  date_now = dateFormat()
  links = []
  url = 'https://www.ibfd.org/news'
  soupers = scrappers(url,'div','col mb-5 views-row')
  for x in soupers:
    tgl = x.find('div','ib-card-date-wrapper ib-card-round d-flex flex-column').text.strip().replace('\n',' ')
    link = 'https://www.ibfd.org'+x.find('div','card-body').find('a')['href']
    links.append(link)
  if types == 'all':
    for page in range(1,30):
        urls = f"https://www.ibfd.org/views/ajax?_wrapper_format=drupal_ajax&view_name=news&view_display_id=page&view_args=&view_path=%2Fnews&view_base_path=news&view_dom_id=a94da1d02f5aa1f1cc2cd938459fcecbc382e94cd5562d22fcf85b9a5aae8868&pager_element=0&page={page}&_drupal_ajax=1&ajax_page_state%5Btheme%5D=corpweb_default&ajax_page_state%5Btheme_token%5D=&ajax_page_state%5Blibraries%5D=adobe_launch%2Fadobe_launch%2Cbetter_exposed_filters%2Fauto_submit%2Cbetter_exposed_filters%2Fgeneral%2Ccookies%2Fcookiesjsr%2Ccookies%2Fcookiesjsr.styles%2Ccore%2Finternal.jquery.form%2Ccorpweb%2Fcards%2Ccorpweb%2Fcookies%2Ccorpweb%2Fglobal-scripts%2Ccorpweb%2Fglobal-styling%2Ccorpweb%2Fhero%2Ccorpweb%2Fselect2%2Ccorpweb_default%2Fglobal%2Cfontawesome%2Ffontawesome.webfonts.brands%2Cfontawesome%2Ffontawesome.webfonts.light%2Cfontawesome%2Ffontawesome.webfonts.regular%2Cfontawesome%2Ffontawesome.webfonts.solid%2Cgoogle_tag%2Fgtag%2Cgoogle_tag%2Fgtag.ajax%2Cibcommon%2Fform-errors%2Ciblogon%2Fiblogon-jscss%2Cibsearch%2Fibsearch-jscss%2Clazy%2Flazy%2Cobfuscate_email%2Fdefault%2Cselect2%2Fselect2%2Cselect2%2Fselect2.i18n.en%2Csystem%2Fbase%2Cviews%2Fviews.module%2Cviews_infinite_scroll%2Fviews-infinite-scroll"
        res = req.get(urls)
        se= res.json()
        for i in se:
            if i['command'] == 'insert':
                if i['data'] != '':
                    res = f'''f{i['data']}'''
                    sop = bs(res,'lxml')
                    s = sop.find_all('a',class_="btn btn-outline-secondary")
                    for i in s:
                        link= 'https://www.ibfd.org'+i.get('href')
                        links.append(link)
  for link in links:
    cont = get_isi(link,'article','container ib-news-detail-wrapper')
    tgl = cont.find('span',class_='d-inline-block ib-color-darkest-red').text.strip().split(' ')
    tgl = f"{tgl[1].replace(',','')}/{tgl[0]}/{tgl[2]}"
    tgl = formatTanggal(tgl,bulan_eng)
    tahun = tgl.split('-')[2]
    if types == 'daily' and tgl == date_now:
        judul = cont.find('h1').text.strip()
        try:
          image = 'https://www.ibfd.org'+cont.find('picture').find('img')['src']
        except:image=''
        isin = cont.find('div','field-body')
        isi = ' '.join([x.text.strip() for x in isin.find_all('p')])
        portal='ibfd'
        inputs = inputCsv(f'{path_files}{date_now}')
        inputs.inputRow([judul,link,tgl,image,isi,portal])
    elif types == 'all' and int(tahun) >= 2023:
        judul = cont.find('h1').text.strip()
        try:
          image = 'https://www.ibfd.org'+cont.find('picture').find('img')['src']
        except:image=''
        isin = cont.find('div','field-body')
        isi = ' '.join([x.text.strip() for x in isin.find_all('p')])
        portal='ibfd'
        inputs = inputCsv(path_files)
        inputs.inputRow([judul,link,tgl,image,isi,portal])
    else:break

  print('[DONE] IBFD has been processed')

def bprdjakarta(types,path_files):
  print('[Process] Scraping BprdJakarta ..')
  stopper = True
  page = 1
  portal = 'bprdjakarta'
  date_now = dateFormat()
  while stopper:
    if page == 10:
      stopper=False
    url = f"https://bprd.jakarta.go.id/berita?page={page}"
    soupers = scrappers(url,'div','col-6 col-lg-4 p-0')
    for x in soupers:
      link = x.find('h5',class_='judul-blog').find('a')['href']
      tgl = x.find('div',class_='ps-product__content tgl-post').find('a').text.strip().replace(' ','/')
      tgl = formatTanggal(tgl,bulan_indo)
      tahun = tgl.split('-')[2]
      if types == 'daily' and tgl == date_now:
        cont = get_isi(link,'div','ps-post__content')
        judul = cont.find('h1','ps-post__title').text.strip()
        image = cont.find('div','ps-blog__banner').find('img')['src']
        conts = cont.find_all('div','col-12 col-md-9 mb-20')[0]
        isi = ' '.join([x.text.strip() for x in conts.find_all('p')])
        inputs = inputCsv(f'{path_files}{date_now}')
        inputs.inputRow([judul,link,tgl,image,isi,portal])
      elif types == 'all' and int(tahun) >= 2023:
        cont = get_isi(link,'div','ps-post__content')
        judul = cont.find('h1','ps-post__title').text.strip()
        image = cont.find('div','ps-blog__banner').find('img')['src']
        conts = cont.find_all('div','col-12 col-md-9 mb-20')[0]
        isi = ' '.join([x.text.strip() for x in conts.find_all('p')])
        inputs = inputCsv(path_files)
        inputs.inputRow([judul,link,tgl,image,isi,portal])
      else:break
    page +=1
  print('[DONE] BprdJakarta has been processed')
  

def belasting(types,path_files):
  print('[Process] Scraping Belasting ..')
  stopper = True
  page = 1
  date_now = dateFormat()
  portal = 'belasting'
  while stopper:
    if types=='all' and page == 94:
      stopper=False
    elif types == 'daily' and page == 3:
      stopper = False
    url = f"https://www.belasting.id/{page}/pajak/"
    link_first = get_isi(url,'div','content-group module-large-single-promo module-large-single-promo').find('h3',class_='size-l').find('a')['href']
    link_second = get_isi(url,'div','content-group tag-latest')
    li = link_second.find('ul','story-frag-list layout-grid grid-3').find_all('li')
    link_list = [x.find('h3').find('a')['href'] for x in li]
    link_list.insert(0,link_first)
    for link in link_list:
      link = link
      cont = get_isi(link,'div','container__column')
      tgl = '/'.join(cont.find('span',class_='vcard').text.strip().split(',')[1].split(' ')[1:4])
      tgl = formatTanggal(tgl,bulan_indo)
      tahun = tgl.split('-')[2]
      if types == 'daily' and tgl == date_now:
        image = cont.find('div','media-item__image-wrapper').find('img')['src']
        judul = cont.find('h3',class_='headline').find('a').text.strip()
        cont_isi = cont.find('div',class_='content-new').find_all('p')
        isi = re.sub(r'^[^-]+- ', '', ' '.join([x.text.strip() for x in cont_isi]))
        inputs = inputCsv(f'{path_files}{date_now}')
        inputs.inputRow([judul,link,tgl,image,isi,portal])
      elif types == 'all' and int(tahun) >= 2023:
        image = cont.find('div','media-item__image-wrapper').find('img')['src']
        judul = cont.find('h3',class_='headline').find('a').text.strip()
        cont_isi = cont.find('div',class_='content-new').find_all('p')
        isi = re.sub(r'^[^-]+- ', '', ' '.join([x.text.strip() for x in cont_isi]))
        inputs = inputCsv(path_files)
        inputs.inputRow([judul,link,tgl,image,isi,portal])
      else:break
    page+=1
  print('[DONE] Belasting has been processed')
    


def fungsi_thread(target,types,path_files):
    if target == 1:
      ddtc(types,path_files)
    elif target == 2:
      ikpi(types,path_files)
    elif target == 3:
      hukumonline(types,path_files)
    elif target == 4:
      ibfd(types,path_files)
    elif target == 5:
      itr(types,path_files)
    elif target == 6:
      bprdjakarta(types,path_files)
    elif target == 7:
      belasting(types,path_files)

def ddtc_ikpi_hukumonline(types,path_files):
  threads = []
  for i in range(1,4):
    thread = threading.Thread(target=fungsi_thread, args=(i,types,path_files))
    thread.start()
    threads.append(thread)
  for thread in threads:
    thread.join()


def ibfd_itr_bprd_belasting(types,path_files):
  threads = []
  for i in range(4,8):
    thread = threading.Thread(target=fungsi_thread, args=(i,types,path_files))
    thread.start()
    threads.append(thread)
  for thread in threads:
    thread.join()

