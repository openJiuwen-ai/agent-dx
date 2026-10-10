#!/usr/bin/env bash
set -euo pipefail
[ "$(uname -m)" = aarch64 ]; [ "$(id -u)" = 0 ]
probe_run=/var/lib/afs-acceptance/network-v67-r2
fault_tag=afs-env-v67-only
rule_tcp=(-s 192.168.109.12 -d 192.168.109.13 -p tcp -m multiport --dports 19566,19567 -m comment --comment "$fault_tag" -j DROP)
rule_udp=(-s 192.168.109.12 -d 192.168.109.13 -p udp --dport 19566 -m comment --comment "$fault_tag" -j DROP)
cleanup() {
 for family in tcp udp; do
  if [ "$family" = tcp ]; then rule=("${rule_tcp[@]}"); else rule=("${rule_udp[@]}"); fi
  if iptables -w 3 -C INPUT "${rule[@]}" 2>/dev/null; then iptables -w 3 -D INPUT "${rule[@]}"; fi
 done
}
case "${1:?mode}" in
 install)
 [ -s "$probe_run/ready.json" ]; [ ! -e "$probe_run/fault-installed" ]
 ! iptables -w 3 -C INPUT "${rule_tcp[@]}" 2>/dev/null
 ! iptables -w 3 -C INPUT "${rule_udp[@]}" 2>/dev/null
 iptables-save > "$probe_run/logs/iptables-before.txt"
 # Start independent cleanup before changing any rule. No live AFS port matches.
 nohup bash "$0" watchdog > "$probe_run/logs/watchdog.log" 2>&1 < /dev/null &
 echo "$!" > "$probe_run/watchdog.pid"
 trap 'cleanup' ERR
 iptables -w 3 -I INPUT 1 "${rule_tcp[@]}"
 iptables -w 3 -I INPUT 1 "${rule_udp[@]}"
 date --iso-8601=ns > "$probe_run/fault-installed"
 iptables -nvxL INPUT > "$probe_run/logs/iptables-injected.txt"
 ;;
 watchdog)
 sleep 25
 cleanup
 date --iso-8601=ns > "$probe_run/logs/watchdog-complete.txt"
 ;;
 inspect) iptables -nvxL INPUT > "$probe_run/logs/iptables-hit.txt" ;;
 restore)
 cleanup
 ! iptables -w 3 -C INPUT "${rule_tcp[@]}" 2>/dev/null
 ! iptables -w 3 -C INPUT "${rule_udp[@]}" 2>/dev/null
 iptables-save > "$probe_run/logs/iptables-restored.txt"
 date --iso-8601=ns > "$probe_run/logs/fault-restored.txt"
 ;;
 *) exit 2 ;;
esac
