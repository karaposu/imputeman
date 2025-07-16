# Normal path might return:
{
    'extract_results': {
        'url1': {'component_type': 'diode', 'voltage': '75V'},
        'url2': {'component_type': 'switching diode', 'voltage': '100V'}
    }
}

# Fast path returns:
{
    'scrape_results': {
        'https://www.digikey.com/...': ScrapeResult(
            data='<html>...raw HTML...</html>',
            cost=0.0025,
            success=True
        )
    }
}