import pandas as pd

# Function to classify TCP packets based on flags
def classify_tcp(syn, ack, fin):
    if syn == 1:
        return 1 if ack == 0 else 0  # New connection if only SYN, else normal
    elif fin == 1:
        return 2  # End connection
    return 0  # Normal packet if no SYN or FIN

# Initialize lists for events, connections, and connection size changes
events = []
connections = []
connection_changes = []

# Load the CSV file
file_path = '/Users/kailianjacy/pcapSource/split_1.csv'  # Change this to your file path
output = '/Users/kailianjacy/pcapSource/'  # Change this to your file path
df_new = pd.read_csv(file_path, header=None)

# Process each row in the dataframe
for index, row in df_new.iterrows():
    protocol = row[0]
    time = row[1]  # Time is already a float
    ip_proto = row[2] if not pd.isna(row[2]) else None
    src_ip = row[3]
    dst_ip = row[4]
    src_port = row[5] if not pd.isna(row[5]) else None
    dst_port = row[6] if not pd.isna(row[6]) else None
    frame_len = row[7]
    syn_flag = row[8] if not pd.isna(row[8]) else 0
    ack_flag = row[9] if not pd.isna(row[9]) else 0
    fin_flag = row[10] if not pd.isna(row[10]) else 0

    # Classify TCP packets and handle events
    if protocol == 'TCP':
        conn_type = classify_tcp(syn_flag, ack_flag, fin_flag)
        if conn_type == 1:
            # TCP packet starting a new connection
            events.append((time, "TCPNewConnections", src_ip, dst_ip))
            if (src_ip, dst_ip) not in connections:
                connections.append((src_ip, dst_ip))
                connection_changes.append((time, len(connections)))
        elif conn_type == 2:
            # TCP packet closing or resetting a connection
            events.append((time, "TCPEndConnections", src_ip, dst_ip))
            if (src_ip, dst_ip) in connections:
                connections.remove((src_ip, dst_ip))
                connection_changes.append((time, len(connections)))
        else:
            # Normal TCP packet
            events.append((time, "TCPRoutingCnt", src_ip, dst_ip))
    elif protocol in ['UDP', 'ICMP']:
        # UDP or ICMP packet, no connection handling
        events.append((time, "NoConnectionPacketsRouting"))
        
    if index % 1000 == 0:
	    print(f"Processed {index} packets")

# Save the events and connection changes to CSV files
events_df = pd.DataFrame(events, columns=['Time', 'EventType', 'SrcIP', 'DstIP'])
events_df.to_csv(output + 'events.csv', index=False)

connection_changes_df = pd.DataFrame(connection_changes, columns=['Time', 'Connections'])
connection_changes_df.to_csv( output + 'output_connections.csv', index=False)
