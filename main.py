import json
import requests
import re
import time
import os
import io
from requests_oauthlib import OAuth1
from oauthlib.oauth1 import SIGNATURE_HMAC_SHA256
from bs4 import BeautifulSoup

MAGENTO_BASE_URL = "https://staging-vdt2zeq-5u54sroenlgea.eu-3.magentosite.cloud/rest/V1/products"
STORE_BASE_URL = "https://staging-vdt2zeq-5u54sroenlgea.eu-3.magentosite.cloud/en/"
MEDIA_BASE_URL = "https://staging-vdt2zeq-5u54sroenlgea.eu-3.magentosite.cloud/media/catalog/product"

CONSUMER_KEY = os.environ.get("MAGENTO_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("MAGENTO_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("MAGENTO_ACCESS_TOKEN")
TOKEN_SECRET = os.environ.get("MAGENTO_TOKEN_SECRET")

VF_API_KEY = os.environ.get("VF_API_KEY") 
VF_PROJECT_ID = os.environ.get("VF_PROJECT_ID")
VF_FILENAME = "claus_catalogo_mestre.txt" 

def clean_html_content(raw_html):
    if not raw_html: return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    for style in soup(["style", "script"]):
        style.decompose()
    text = soup.get_text(separator=" ")
    text = re.sub(r'#html-body.*?}', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def get_custom_attribute(item, code):
    attributes = item.get('custom_attributes', [])
    for attribute in attributes:
        if attribute.get('attribute_code') == code:
            return attribute.get('value')
    return None

def check_filters(item, ignore_stock=True):
    if item.get('status') != 1:
        return False, f"Status:Disabled({item.get('status')})"

    # 2. Filtro de Tipo (Apenas Simple Products)
    if item.get('type_id') != 'simple':
        return False, f"Type:{item.get('type_id')}"
    
    # 3. Filtro de Visibilidade (4 = Catalog, Search)
    visibility = item.get('visibility')
    if str(visibility) != '4':
        return False, f"Vis:{visibility}"
    
    # 4. Filtro de Stock (Mantido opcional porque a API Staging reporta 0)
    ext_attr = item.get('extension_attributes', {})
    stock_item = ext_attr.get('stock_item', {})
    qty = stock_item.get('qty', 0)
    in_stock = stock_item.get('is_in_stock', False)
    
    has_stock = qty > 0 and in_stock
    
    if not ignore_stock and not has_stock:
        return False, f"NoStock(Qty:{qty})"
    
    return True, "OK"

def determine_official_category(name):
    name_lower = name.lower()
    if any(x in name_lower for x in ['eau de toilette', 'eau de cologne', 'parfum', 'fragrance', 'cologne', 'scent']):
        return 'PERFUMARIA (Fragrance)'
    elif any(x in name_lower for x in ['soap', 'sabonete', 'body wash', 'hand wash', 'gel']):
        return 'SABONETE/BANHO (Soap)'
    elif any(x in name_lower for x in ['candle', 'diffuser', 'vela', 'difusor']):
        return 'CASA (Home)'
    elif any(x in name_lower for x in ['cream', 'lotion', 'oil', 'body', 'hand cream']):
        return 'CORPO (Body Care)'
    elif 'shaving' in name_lower or 'barbear' in name_lower:
        return 'BARBA (Grooming)'
    else:
        return 'OUTROS (General)'

def fetch_all_products():
    oauth = OAuth1(CONSUMER_KEY, client_secret=CONSUMER_SECRET,
                   resource_owner_key=ACCESS_TOKEN, resource_owner_secret=TOKEN_SECRET,
                   signature_method=SIGNATURE_HMAC_SHA256)
    all_products = []
    page, page_size = 1, 100
    
    while True:
        params = {"searchCriteria[pageSize]": page_size, "searchCriteria[currentPage]": page}
        try:
            response = requests.get(MAGENTO_BASE_URL, auth=oauth, params=params)
            if response.status_code != 200:
                print(f"ERRO MAGENTO: {response.status_code}")
                break
            items = response.json().get('items', [])
            if not items: break
            all_products.extend(items)
            if len(items) < page_size: break
            page += 1
        except Exception as e:
            print(f"ERRO CONEXÃO: {e}")
            break
    return all_products

def process_products_to_structured_text(products):
    text_content = ""
    valid, skipped = 0, 0
    rejection_reasons = {}

    for p in products:
        # ATENÇÃO: ignore_stock=True mantido para contornar bug da API Staging
        is_valid, reason = check_filters(p, ignore_stock=True)
        
        if not is_valid:
            skipped += 1
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
            continue

        valid += 1
        name = p.get('name', 'N/A')
        sku = p.get('sku', 'N/A')
        price = p.get('price', 0)
        cat = determine_official_category(name)
        url_key = get_custom_attribute(p, 'url_key')
        image = get_custom_attribute(p, 'image')
        desc = clean_html_content(get_custom_attribute(p, 'description'))
        short = clean_html_content(get_custom_attribute(p, 'short_description'))
        ing = clean_html_content(get_custom_attribute(p, 'ingredients'))
        
        # --- CORREÇÃO UTM IMPLEMENTADA AQUI ---
        if url_key:
            link = f"{STORE_BASE_URL}{url_key}?utm_source=chat&utm_medium=product_chatbot"
        else:
            link = "N/A"
            
        img_link = f"{MEDIA_BASE_URL}{image}" if image and image != 'no_selection' else "N/A"

        block = f"--- INÍCIO DE PRODUTO ---\nNOME: {name}\nCATEGORIA_OFICIAL: {cat}\nSKU: {sku}\nPREÇO: {price} USD\nLINK: {link}\nIMAGEM: {img_link}\n"
        if short: block += f"RESUMO: {short}\n"
        if desc: block += f"DESCRIÇÃO: {desc}\n"
        if ing: block += f"\n[DADOS_TECNICOS_INGREDIENTES]: {ing}\n"
        block += "--- FIM DE PRODUTO ---\n\n"
        text_content += block
            
    print(f" > Resultados: {valid} válidos / {skipped} rejeitados.")
    if skipped > 0:
        print(" > Motivos de rejeição (Top 3):")
        for r, count in sorted(rejection_reasons.items(), key=lambda x: x[1], reverse=True)[:3]:
            print(f"   - {r}: {count}")

    return text_content

def delete_old_documents():
    headers = {"Authorization": VF_API_KEY}
    url = f"https://api.voiceflow.com/v1/knowledge-base/docs?projectID={VF_PROJECT_ID}"
    try:
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            for doc in res.json().get('data', []):
                if VF_FILENAME in doc.get('name', ''):
                    requests.delete(f"https://api.voiceflow.com/v1/knowledge-base/docs/{doc['documentID']}?projectID={VF_PROJECT_ID}", headers=headers)
    except: pass

def upload_to_voiceflow(text_data):
    if not text_data:
        print("ERRO CRÍTICO: Ficheiro vazio. Nenhum produto passou nos filtros.")
        return None
    url = f"https://api.voiceflow.com/v1/knowledge-base/docs/upload?projectID={VF_PROJECT_ID}&overwrite=true"
    headers = {"Authorization": VF_API_KEY}
    files = {'file': (VF_FILENAME, io.BytesIO(text_data.encode('utf-8')), 'text/plain')}
    return requests.post(url, headers=headers, files=files)

if __name__ == "__main__":
    raw = fetch_all_products()
    if raw:
        final_text = process_products_to_structured_text(raw)
        if final_text:
            delete_old_documents()
            res = upload_to_voiceflow(final_text)
            if res and res.status_code == 200: print("SUCESSO: Base de dados atualizada.")
            else: print(f"ERRO UPLOAD: {res.status_code if res else 'N/A'}")
