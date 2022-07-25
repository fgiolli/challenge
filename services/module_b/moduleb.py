import redis
import json
import time
import csv
  
head = ['id_customer', 'discount']

DAYS = ['28','29','30','31']

if __name__ == "__main__":
    r = redis.Redis(host='redis-14047.c17.us-east-1-4.ec2.cloud.redislabs.com', port=14047, db=0, password='SwgBhJqaRP6Qph0KCjqyGfBH3kuCiiO8')

    while True:
        data = r.get('payments')
        if not data:
            time.sleep(2)
        else:
            break
    while True:
        dates = r.get('dates')
        if not dates:
            time.sleep(2)
        else:
            r.flushall()
            break

    dates = dates.decode()

    data = json.loads(str(data.decode()))
    output = list()
    for customer in data:
        date_split = dates.split('-')
        day = date_split[2]
        month = date_split[1]
        year = date_split[0]
        last_month = str(int(month)-1)
        for day in DAYS:
            try:
                if customer.get('payments').get(f'{year}-{last_month}-{day}') > 10000:
                    output.append({
                        'id_customer':customer.get('id_customer'),
                        'discount': 30
                        })
            except Exception:
                pass
            try:
                if customer.get('payments').get(f'{year}-{last_month}-{day}') > 5000:
                    output.append({
                        'id_customer':customer.get('id_customer'),
                        'discount': 20
                        })
            except Exception:
                pass
        if customer.get('job').lower() in ['management','admin.']:
            output.append({
                'id_customer':customer.get('id_customer'),
                'discount': 30
                })
        elif customer.get('job').lower() in ['blue-collar','technician','entrepeneur']:
            output.append({
                'id_customer':customer.get('id_customer'),
                'discount': 20
                })
    with open(f'../../output/output_{dates}.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames = head)
        writer.writeheader()
        writer.writerows(output)
            
            