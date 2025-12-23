"""
Menu Extraction Op using Google Gemini (New SDK)
"""
import os
import re
import json
import time
from pdf2image import convert_from_path
from io import BytesIO
import pandas as pd
from google import genai
from google.genai import types
from dagster import op, Out, In, Field, String

@op(
    config_schema={
        "gemini_api_key": Field(
            String, 
            description="Google Gemini API Key",
            default_value=os.getenv("GEMINI_API_KEY", "AIzaSyCj7FgZ_MQIEdKIBrOvHxRZVqzKTvENTvQ")
        ),
        "gemini_model": Field(String, default_value="gemini-2.0-flash-exp"),
        "dpi": Field(int, default_value=200),
        "max_retries": Field(int, default_value=3),
    },
    ins={"pdf_path": In(str, description="Path to PDF file")},
    out=Out(pd.DataFrame, description="Extracted menu data"),
    name="extract_menu_with_gemini_op",
    description="Extract menu items from PDF using Google Gemini AI"
)
def extract_menu_with_gemini_op(context, pdf_path: str) -> pd.DataFrame:
    """Extract menu items from PDF using Gemini"""
    
    config = context.op_config
    
    # Log the input
    context.log.info(f"Starting Gemini extraction for: {pdf_path}")
    
    # Configure Gemini client (new SDK)
    client = genai.Client(api_key=config["gemini_api_key"])
    
    # Convert PDF to images
    context.log.info(f"Converting PDF: {pdf_path}")
    try:
        images = convert_from_path(pdf_path, dpi=config["dpi"])
        context.log.info(f"Converted {len(images)} pages")
    except Exception as e:
        context.log.error(f"Failed to convert PDF: {e}")
        return pd.DataFrame(columns=['name', 'category', 'subcategory', 'description', 'price'])
    
    # Extraction prompt
    prompt = """Analyze this menu image and extract ALL menu items into a JSON array.

RULES:
1. **Name**: Include size in name if multiple sizes exist
2. **Category**: Main section (Pizza, Appetizers, Desserts, etc.)
3. **Subcategory**: Subsection - NEVER empty
4. **Description**: Use from menu OR generate - NEVER empty
5. **Price**: Numeric only (34000 not "34,000"), null if unavailable

OUTPUT: JSON array only, no markdown.
[{"name":"Item","category":"Cat","subcategory":"Sub","description":"Desc","price":10.99},...]"""
    
    all_items = []
    
    # Process each page
    for page_num, image in enumerate(images, 1):
        context.log.info(f"Processing page {page_num}/{len(images)}")
        
        for attempt in range(1, config["max_retries"] + 1):
            try:
                # Convert PIL image to bytes for new SDK
                img_byte_arr = BytesIO()
                image.save(img_byte_arr, format='PNG')
                img_byte_arr = img_byte_arr.getvalue()
                
                # Use new SDK API
                response = client.models.generate_content(
                    model=config["gemini_model"],
                    contents=[
                        types.Part.from_text(text=prompt),
                        types.Part.from_bytes(data=img_byte_arr, mime_type="image/png")
                    ],
                    config=types.GenerateContentConfig(
                        temperature=0.1,
                        max_output_tokens=16384
                    )
                )
                
                # Parse JSON
                items = parse_json_safe(response.text)
                
                if items:
                    context.log.info(f"  ✓ Found {len(items)} items on page {page_num}")
                    all_items.extend(items)
                    break
                else:
                    context.log.warning(f"  ⚠ No items parsed on attempt {attempt}")
                    
            except Exception as e:
                context.log.warning(f"  ✗ Attempt {attempt} failed: {e}")
                if attempt < config["max_retries"]:
                    time.sleep(2)
    
    # Clean and create DataFrame
    cleaned_items = validate_and_clean_items(all_items)
    context.log.info(f"Total extracted: {len(cleaned_items)} items")
    
    if not cleaned_items:
        context.log.warning("No items extracted from PDF")
        return pd.DataFrame(columns=['name', 'category', 'subcategory', 'description', 'price'])
    
    df = pd.DataFrame(cleaned_items)
    df = df[['name', 'category', 'subcategory', 'description', 'price']]
    df = df.drop_duplicates(subset=['name', 'price'], keep='first')
    
    context.log.info(f"Final DataFrame: {len(df)} unique items")
    context.log.info(f"Categories: {df['category'].unique().tolist()}")
    
    return df


def parse_json_safe(json_str: str) -> list:
    """Parse JSON with fallback strategies"""
    # Remove markdown
    json_str = re.sub(r'^```json\s*', '', json_str.strip())
    json_str = re.sub(r'^```\s*', '', json_str)
    json_str = re.sub(r'\s*```$', '', json_str).strip()
    
    # Try direct parse
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        pass
    
    # Try regex extraction
    try:
        pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
        items = []
        for match in re.findall(pattern, json_str):
            try:
                obj = json.loads(match)
                if isinstance(obj, dict) and 'name' in obj:
                    items.append(obj)
            except json.JSONDecodeError:
                continue
        return items
    except Exception:
        pass
    
    return []


def validate_and_clean_items(items: list) -> list:
    """Validate and clean extracted items"""
    cleaned = []
    
    for item in items:
        if not isinstance(item, dict) or not item.get('name'):
            continue
        
        clean_item = {
            'name': item.get('name'),
            'category': item.get('category'),
            'subcategory': item.get('subcategory') or 'General',
            'description': item.get('description') or f"Delicious {item.get('name')}",
            'price': item.get('price')
        }
        
        # Clean price
        if clean_item['price'] is not None:
            try:
                price_str = re.sub(r'[^\d.]', '', str(clean_item['price']))
                clean_item['price'] = float(price_str) if price_str else None
            except (ValueError, TypeError):
                clean_item['price'] = None
        
        cleaned.append(clean_item)
    
    return cleaned
