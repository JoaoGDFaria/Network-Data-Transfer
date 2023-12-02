;
; BIND data file for local loopback interface
;
$TTL	604800
@	IN	SOA	cc.com. root.cc.com. (
			      3		; Serial
			 604800		; Refresh
			  86400		; Retry
			2419200		; Expire
			 604800 )	; Negative Cache TTL
; name servers - NS records
	IN	NS	ns1.cc.com.
	IN	NS	ns2.cc.com.

; name servers - A records
ns1.cc.com.	IN	A	10.0.17.10
ns2.cc.com.	IN	A	10.0.19.10

; 
node1.cc.com.	IN	A	10.0.15.20
node2.cc.com.	IN	A	10.0.16.20
fstracker.cc.com.	IN	A	10.0.12.10
