package kafka.config;

public class DNSLookupMode {
    public static final String DEFAULT = "default";
    public static final String USE_ALL_DNS_IPS = "use_all_dns_ips";
    public static final String RESOLVE_CANONICAL_BOOTSTRAP_SERVERS_ONLY = "resolve_canonical_bootstrap_servers_only";

    private DNSLookupMode() {
    }
}
