import json
import requests
import re
import time
import os
import io
from requests_oauthlib import OAuth1
from oauthlib.oauth1 import SIGNATURE_HMAC_SHA256
from bs4 import BeautifulSoup

# Configuração EN
STORE_VIEW = "en"
WAREHOUSE = "warehouse_pt"  # EN usa o mesmo warehouse que PT
MAGENTO_BASE_URL = f"https://clausporto.com/{STORE_VIEW}/rest/V1/products"
STOCK_BASE_URL = f"https://clausporto.com/index.php/rest/{STORE_VIEW}/V1/inventory/source-items"
STORE_BASE_URL = "https://clausporto.com/en/" 
MEDIA_BASE_URL = "https://clausporto.com/media/catalog/product"

CONSUMER_KEY = os.environ.get("MAGENTO_CONSUMER_KEY")
CONSUMER_SECRET = os.environ.get("MAGENTO_CONSUMER_SECRET")
ACCESS_TOKEN = os.environ.get("MAGENTO_ACCESS_TOKEN")
TOKEN_SECRET = os.environ.get("MAGENTO_TOKEN_SECRET")

VF_API_KEY = os.environ.get("VF_API_KEY") 
VF_PROJECT_ID = os.environ.get("VF_PROJECT_ID")
VF_FILENAME = "claus_catalogo_en.txt" 

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

def check_filters(item, stock_info=None):
    """
    Filtra produtos com base em:
    - Status ativo
    - Tipo simple
    - Visibilidade 4
    - Stock disponível (se fornecido)
    """
    if item.get('status') != 1:
        return False, f"Status:Disabled({item.get('status')})"
    if item.get('type_id') != 'simple':
        return False, f"Type:{item.get('type_id')}"
    visibility = item.get('visibility')
    if str(visibility) != '4':
        return False, f"Vis:{visibility}"
    
    # Verificar stock se fornecido
    if stock_info:
        qty = stock_info.get('quantity', 0)
        status = stock_info.get('status', 0)
        if qty <= 0 or status != 1:
            return False, f"NoStock(Qty:{qty},Status:{status})"
    
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
    """Busca todos os produtos da store view EN"""
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
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            print(f"ERRO CONEXÃO: {e}")
            break
    return all_products

def fetch_stock_for_skus(sku_list):
    """
    Busca informação de stock para uma lista de SKUs
    Retorna dict {sku: {quantity: X, status: Y, source_code: Z}}
    """
    if not sku_list:
        return {}
    
    oauth = OAuth1(CONSUMER_KEY, client_secret=CONSUMER_SECRET,
                   resource_owner_key=ACCESS_TOKEN, resource_owner_secret=TOKEN_SECRET,
                   signature_method=SIGNATURE_HMAC_SHA256)
    
    # Dividir em chunks de 50 SKUs por pedido
    chunk_size = 50
    all_stock_data = {}
    
    for i in range(0, len(sku_list), chunk_size):
        chunk = sku_list[i:i + chunk_size]
        sku_string = ",".join(chunk)
        
        params = {
            "searchCriteria[filter_groups][0][filters][0][field]": "sku",
            "searchCriteria[filter_groups][0][filters][0][condition_type]": "in",
            "searchCriteria[filter_groups][0][filters][0][value]": sku_string
        }
        
        try:
            response = requests.get(STOCK_BASE_URL, auth=oauth, params=params)
            if response.status_code != 200:
                print(f"AVISO: Erro ao buscar stock (status {response.status_code})")
                continue
                
            items = response.json().get('items', [])
            
            # Organizar por SKU e filtrar pelo warehouse correto
            for item in items:
                sku = item.get('sku')
                source = item.get('source_code')
                
                # EN usa warehouse_pt
                if source == WAREHOUSE:
                    all_stock_data[sku] = {
                        'quantity': item.get('quantity', 0),
                        'status': item.get('status', 0),
                        'source_code': source
                    }
            
            time.sleep(0.3)  # Rate limiting
            
        except Exception as e:
            print(f"AVISO: Erro ao processar stock: {e}")
            continue
    
    return all_stock_data

def process_products_to_structured_text(products, stock_data):
    """
    Processa produtos e cria texto estruturado incluindo informação de stock
    """
    text_content = ""
    valid, skipped = 0, 0
    rejection_reasons = {}
    images_found = 0
    with_stock = 0

    for p in products:
        sku = p.get('sku', 'N/A')
        stock_info = stock_data.get(sku)
        
        # Aplicar filtros incluindo stock
        is_valid, reason = check_filters(p, stock_info)
        if not is_valid:
            skipped += 1
            rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1
            continue

        valid += 1
        if stock_info and stock_info.get('quantity', 0) > 0:
            with_stock += 1
            
        name = p.get('name', 'N/A')
        price = p.get('price', 0)
        cat = determine_official_category(name)
        url_key = get_custom_attribute(p, 'url_key')
        
        # Fallback para imagens
        image = get_custom_attribute(p, 'image')
        if not image or image == 'no_selection':
            image = get_custom_attribute(p, 'small_image')
        if not image or image == 'no_selection':
            image = get_custom_attribute(p, 'thumbnail')
            
        desc = clean_html_content(get_custom_attribute(p, 'description'))
        short = clean_html_content(get_custom_attribute(p, 'short_description'))
        ing = clean_html_content(get_custom_attribute(p, 'ingredients'))
        
        if url_key:
            link = f"{STORE_BASE_URL}{url_key}?sku={sku}&utm_source=chatbot&utm_medium=product_chatbot"
        else:
            link = "N/A"
        
        if image and image != 'no_selection' and image.strip():
            image_clean = image.lstrip('/')
            img_link = f"{MEDIA_BASE_URL}/{image_clean}"
            images_found += 1
        else:
            img_link = "N/A"

        # Informação de stock
        stock_qty = stock_info.get('quantity', 0) if stock_info else 0
        stock_status = "EM STOCK" if stock_qty > 0 else "SEM STOCK"
        
        block = f"--- INÍCIO DE PRODUTO ---\n"
        block += f"NOME: {name}\n"
        block += f"SKU: {sku}\n"
        block += f"CATEGORIA_OFICIAL: {cat}\n"
        block += f"PREÇO: {price} EUR\n"
        block += f"STOCK: {stock_status} (Quantidade: {stock_qty})\n"
        block += f"LINK: {link}\n"
        block += f"IMAGEM: {img_link}\n"
        if short: block += f"RESUMO: {short}\n"
        if desc: block += f"DESCRIÇÃO: {desc}\n"
        if ing: block += f"\n[DADOS_TECNICOS_INGREDIENTES]: {ing}\n"
        block += "--- FIM DE PRODUTO ---\n\n"
        text_content += block
            
    print(f" > Resultados: {valid} válidos / {skipped} rejeitados / {with_stock} com stock / {images_found} com imagem.")
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
    if not CONSUMER_KEY or not VF_API_KEY:
        print("ERRO: Variáveis de ambiente em falta.")
        exit(1)
        
    print(f"========================================")
    print(f"CLAUS PORTO - Atualização EN (Warehouse: {WAREHOUSE})")
    print(f"========================================\n")
    
    print("1. Magento (EN): A carregar produtos...")
    raw = fetch_all_products()
    
    if not raw:
        print("Erro: Nenhum produto recebido do Magento.")
        exit(1)
    
    print(f"   > Sucesso: {len(raw)} produtos carregados.\n")
    
    # Extrair lista de SKUs e buscar stock
    print("2. Magento: A carregar informação de stock...")
    all_skus = [p.get('sku') for p in raw if p.get('sku')]
    print(f"   > A verificar stock para {len(all_skus)} SKUs...")
    
    stock_data = fetch_stock_for_skus(all_skus)
    print(f"   > Stock carregado para {len(stock_data)} produtos do {WAREHOUSE}.\n")
    
    print("3. A processar produtos...")
    final_text = process_products_to_structured_text(raw, stock_data)
    
    if not final_text:
        print("ERRO: Nenhum produto passou nos filtros.")
        exit(1)
    
    print("\n4. Voiceflow: A limpar base de dados antiga...")
    delete_old_documents()
    
    print("\n5. Voiceflow: A enviar nova versão EN...")
    res = upload_to_voiceflow(final_text)
    
    if res and res.status_code == 200: 
        print("\n✓ SUCESSO: Base de dados EN atualizada com stock!")
    else: 
        print(f"\n✗ ERRO UPLOAD: {res.status_code if res else 'N/A'}")
        if res:
            print(res.text)
        exit(1)
