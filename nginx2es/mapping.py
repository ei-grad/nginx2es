DEFAULT_TEMPLATE = {
    "template": "nginx-*",
    "settings": {
        "index.refresh_interval": "10s",
        "index.unassigned.node_left.delayed_timeout": "5m",
    },
    "mappings": {
        "_default_": {
            "_all": {"enabled": False},
            "date_detection": False,
            "dynamic_templates": [
                {
                    "string_fields": {
                        "match": "*",
                        "match_mapping_type": "string",
                        "mapping": {"type": "keyword", "norms": False}
                    }
                },
                {
                    "long_fields": {
                        "match": "*",
                        "match_mapping_type": "long",
                        "mapping": {"type": "long", "norms": False}
                    }
                }
            ],
            "properties": {
                "@timestamp": {"type": "date", "format": "dateOptionalTime"},
                "remote_addr": {"type": "ip"},
                "geoip": {"type": "geo_point"},
                "query_geo": {"type": "geo_point"},
                "status": {"type": "long"},
                "request": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword", "norms": False}
                    }
                },
                "request_path": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword", "norms": False}
                    }
                },
                "request_qs": {
                    "type": "text",
                    "fields": {
                        "raw": {"type": "keyword", "norms": False}
                    }
                }
            }
        }
    }
}
