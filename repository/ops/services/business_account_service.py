import requests
import os
from functools import lru_cache
import logging

logger = logging.getLogger(__name__)


def update_business_account_is_indexed(context, id, is_indexed):
    """
    Update the is_indexed flag for a business account.
    """
    backend_external_token = os.getenv("BACKEND_EXTERNAL_TOKEN")
    backend_host_url = os.getenv("BACKEND_HOST_URL")
    
    try:
        response = requests.put(
            f'{backend_host_url}/business-accounts/{id}',
            headers={
                'x-external-token': backend_external_token,
                'Content-Type': 'application/json'
            },
            json={
                "is_indexed": is_indexed
            }
        )
        
        if response.status_code == 200:
            context.log.info('Successfully updated the business account!')
            return 'Successfully updated the business account!'
        else:
            context.log.error(f'Failed to update the business account: {response.text}')
            raise Exception(f'Failed to update the business account: {response.text}')
    except Exception as e:
        context.log.error(f'Error occurred while updating business account {id}: {e}')
        raise


@lru_cache(maxsize=128)
def get_business_details(business_id: str) -> dict:

    backend_external_token = os.getenv("BACKEND_EXTERNAL_TOKEN")
    backend_host_url = os.getenv("BACKEND_HOST_URL")
    
    if not backend_external_token or not backend_host_url:
        raise RuntimeError("BACKEND_EXTERNAL_TOKEN or BACKEND_HOST_URL not set")
    
    url = f"{backend_host_url}/business-accounts/{business_id}"
    
    logger.info(f"Fetching business details from: {url}")
    
    try:
        response = requests.get(
            url,
            headers={
                "x-external-token": backend_external_token,
                "Content-Type": "application/json",
            },
            timeout=10,
        )
        
        response.raise_for_status()
        
        # Parse the nested response structure
        result = response.json()
        
        # API returns: {success: true, data: {business_account: {...}}}
        if not result.get('success'):
            raise RuntimeError(f"API returned success=false for business {business_id}")
        
        data = result.get('data', {})
        business_account = data.get('business_account', {})
        
        if not business_account:
            raise ValueError(f"No business_account data found in response for {business_id}")
        
        # Extract industry information
        industry = business_account.get('industry', {})
        industry_type_id = industry.get('id') if industry else None
        industry_name = industry.get('name') if industry else None
        
        # Build simplified response for easier consumption
        simplified = {
            'id': business_account.get('id'),
            'name': business_account.get('name'),
            'email': business_account.get('email'),
            'mobile': business_account.get('mobile'),
            'country_code': business_account.get('country_code'),
            'country': business_account.get('country'),
            'address': business_account.get('address'),
            'logo': business_account.get('logo'),
            'website_domain': business_account.get('website_domain'),
            'industry_type': industry_type_id,
            'industry_name': industry_name,
            'created_at': business_account.get('created_at'),
            'updated_at': business_account.get('updated_at'),
        }
        
        logger.info(
            f"✓ Found business: {simplified['name']} "
            f"(industry={industry_name})"
        )
        
        return simplified
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            raise ValueError(
                f"Business account {business_id} not found. "
                f"Please verify the ID exists in the database."
            )
        raise RuntimeError(f"API request failed: {e}")
    
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to connect to API at {backend_host_url}: {e}")


# Business industry IDs (from database constants)
INDUSTRY_TYPE_MAP = {
    "0199947b-b0a0-7885-a32a-fff1a453cae9": "grocery",
    "0199947b-b0a0-7885-a32a-f038747fc002": "restaurant",
}


def resolve_industry_type(industry_type_id: str) -> str:
    """
    Convert industry_type UUID → readable business type.
    
    Args:
        industry_type_id: UUID from industry_list table
        
    Returns:
        "grocery", "restaurant", or "unknown"
    """
    if not industry_type_id:
        return "unknown"
    return INDUSTRY_TYPE_MAP.get(industry_type_id, "unknown")


def get_business_type(business_id: str) -> str:
    """
    Convenience function to get just the business type (grocery/restaurant).
    
    Args:
        business_id: UUID of the business account
        
    Returns:
        "grocery", "restaurant", or "unknown"
    """
    try:
        details = get_business_details(business_id)
        industry_type_id = details.get("industry_type")
        return resolve_industry_type(industry_type_id)
    except Exception as e:
        logger.error(f"Failed to get business type for {business_id}: {e}")
        return "unknown"


# Testing/debugging
if __name__ == "__main__":
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) > 1:
        business_id = sys.argv[1]
        
        print(f"\n{'='*80}")
        print(f"Testing business_account_service.py")
        print(f"{'='*80}")
        print(f"Business ID: {business_id}\n")
        
        try:
            details = get_business_details(business_id)
            
            print(f"✅ SUCCESS - Business Found\n")
            print(f"📋 Details:")
            print(f"   ID: {details.get('id')}")
            print(f"   Name: {details.get('name')}")
            print(f"   Email: {details.get('email')}")
            print(f"   Country: {details.get('country')}")
            print(f"   Mobile: {details.get('country_code')} {details.get('mobile')}")
            
            print(f"\n🏭 Industry Information:")
            print(f"   Industry Type ID: {details.get('industry_type')}")
            print(f"   Industry Name: {details.get('industry_name')}")
            
            industry_type = resolve_industry_type(details.get('industry_type'))
            print(f"   Resolved Type: {industry_type.upper()}")
            
            if industry_type == "grocery":
                print(f"   🛒 This is a GROCERY business")
            elif industry_type == "restaurant":
                print(f"   🍽️  This is a RESTAURANT business")
            else:
                print(f"   ❓ Unknown industry type")
            
            print(f"\n{'='*80}\n")
            
        except ValueError as e:
            print(f"❌ ERROR - {str(e)}\n")
            sys.exit(1)
        except RuntimeError as e:
            print(f"❌ ERROR - {str(e)}\n")
            sys.exit(1)
        except Exception as e:
            print(f"❌ UNEXPECTED ERROR - {type(e).__name__}: {str(e)}\n")
            sys.exit(1)
    else:
        print("\n📖 Usage:")
        print(f"   python {sys.argv[0]} <business_id>\n")
        print("📝 Example:")
        print(f"   python {sys.argv[0]} f584a82e-35e0-4072-994f-b849dd88690f\n")
