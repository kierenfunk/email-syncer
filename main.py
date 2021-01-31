from datetime import datetime
from mercuryV1 import Mercury
from mailchimp3 import MailChimp
import psycopg2
import credentials as creds


# starter code from:
# https://saqibameen.com/deploy-python-cron-job-scripts-on-heroku/

def get_mailchimp_data(mailchimp_client,list_id):
	""" Retrieve members in mailchimp and return only necessary data
	
	Parameters
	----------
	mailchimp_client : mailchimp object
		make connections with the mailchimp API
	list_id : string
		id of the member list
	
	Returns
	-------
	dict
		each email address with corresponding member id and subscribed status
	"""
	data = mailchimp_client.lists.members.all(list_id, get_all=True)
	return {member['email_address'].lower():{"unique_id":member['id'],"unsubscribed":(member['status']=='unsubscribed'),"cleaned":member['status']=='cleaned'} for member in data['members'] if member['status'] != 'pending'}

def get_mercury_data(mercury_client):
	""" Retrieve contacts from mercury and return only necessary data
	
	Parameters
	----------
	mercury_client : mercury object
		make connections with the mailchimp API
	
	Returns
	-------
	dict
		each email address with corresponding unique id's and subscribed statuses
	"""
	data = mercury_client.contacts.get()
	# remove contacts without an email field and that are marked as deleted.
	data = [contact for contact in data if 'email' in contact and not contact['isDeleted']]
	# remove contacts with a blank email field or invalid email address
	data = [contact for contact in data if len(contact['email']) > 0 and '@' in contact['email']]

	# put data into a dictionary and reduce contacts to their unique id, email and subscribe status
	result = dict()
	for contact in data:
		email = contact['email'].strip(' ').lower()
		if email not in result:
			result[email] = list()
		result[email].append({"unique_id":contact['uniqueId'],"unsubscribed":contact['doNotMail']})
	return result
	
def is_conflict(contact_list):
	""" helper function for identifying if there is a conflict of subscribe status
	
	Parameters
	----------
	contact_list : list
		a list of contacts with the same email
	
	Returns
	-------
	boolean
		true if there is a conflict
	"""
	status_sum = sum([int(contact['unsubscribed']) for contact in contact_list])
	# if not all statuses false and not all statues true, return true
	return status_sum!=0 and status_sum != len(contact_list)

def mercury_presync(mercury_data, status, mercury_client):
	""" resolve subscribe status conflicts amongst contacts with the same email address
	
	Parameters
	----------
	mercury_data : dict
		data from mercury
	status : dict
		data from the status
	mercury_client : mercury object
		passed to update_mercury_contact()
	Returns
	-------
	dict
		original mercury_data with conflicts now resolved
	"""

	# get a list of emails that have conflicts
	conflicts = [email for (email,contact_list) in mercury_data.items() if is_conflict(contact_list)]

	# go through each conflict and resolve conflicts
	for email in conflicts:
		# set to True for conflict resolution (if email does not exist in the status, both will become False, aka subscribed)
		past_email_status = True
		if email in status:
			past_email_status = status[email]

		for contact in mercury_data[email]:
			if contact['unsubscribed'] == past_email_status:
				# change contacts that are the same as status i.e contacts that haven't been updated
				try:
					mercury_client.contacts.update(id=contact['unique_id'], data={"doNotMail":not past_email_status})
					contact['unsubscribed'] = not past_email_status
					print({'email':email,'message':"the doNotMail status was updated in mercury"})
				except:
					print({'email':email,'message':"there was a conflict with two contacts in mercury, it could not be resolved"})
	return mercury_data

def get_client_name(mercury):
	""" small helper function """
	if mercury:
		return "mercury"
	return "mailchimp"

def mailchimp_status_convert(status):
	""" small helper function """
	if status:
		return 'unsubscribed'
	return 'subscribed'

def create_emails(email_set, client, data, mercury):
	""" Create new email entries in mailchimp/mercury with API
	
	Parameters
	----------
	email_set : Set
		list of emails to create
	client : Object
		client for communicating with appropriate API
	data : Dict
		all data to be used to create new contacts/members
	mercury : Bool
		Used to determine which API to communicate with
	"""

	for email in email_set:
		try:
			if mercury:
				client.contacts.create(data={"firstName":' ',"lastName":' ',"contactMethods":[{"contactMethod":"Email 1","content":email}],"doNotMail":data[email]['unsubscribed']})
				change_db('insert',email,data[email]['unsubscribed'])
				print({'email':email,'message':'was added to mercury successfully'})
			else:
				client.lists.members.create(creds.mailchimp['list_id'], {'email_address': email,'status': mailchimp_status_convert(data[email][0]['unsubscribed'])})
				change_db('insert',email,data[email][0]['unsubscribed'])
				print({'email':email,'message':'was added to mailchimp successfully'})
		except:
			print({'email':email,'message':'was NOT added to {}'.format(get_client_name(mercury))})

def delete_emails(email_set, client, data):
	""" Delete email entries in mailchimp/mercury with API
	
	Parameters
	----------
	email_set : Set
		list of emails to delete
	client : Object
		client for communicating with appropriate API
	data : Dict
		all data to be used to delete new contacts/members
	"""

	for email in email_set:
		try:
			# in some cases the email has already been deleted in mailchimp by admin (this can occur sometimes but also allows to clear cleaned members from mailchimp)
			if email in data:
				if data[email]['cleaned']:
					client.lists.members.delete_permanent(list_id=creds.mailchimp['list_id'], subscriber_hash=data[email]['unique_id'])
				client.lists.members.delete(list_id=creds.mailchimp['list_id'], subscriber_hash=data[email]['unique_id'])
			change_db('delete',email)
			print({'email':email,'message':'was deleted from mailchimp successfully'})
		except:
			print({'email':email,'message':'was NOT deleted from mailchimp'})

def unsubscribe_mercury_emails(email_set,status,data,client):
	""" Unsubscribe email entries in mercury that have been deleted in mailchimp
	
	Parameters
	----------
	email_set : Set
		list of emails to unsubscribe
	status : Dict
		status data to be used to unsubscribe contacts
	data : Dict
		all data to be used to unsubscribe contacts
	client : Object
		client for communicating with mercury API
	"""
	for email in email_set:
		if status[email] != True or data[email][0]['unsubscribed'] != True:
			try:
				client.contacts.update(id=data[email][0]['unique_id'], data={"doNotMail":True})
				change_db('update',email,True)
				print({'email':email,'message':'was unsubscribed from mercury successfully'})
			except:
				print({'email':email,'message':'was NOT unsubscribed from mercury'})


def resolve_conflicts(conflicts,status,mercury_data,mailchimp_data,mercury_client,mailchimp_client):
	""" resolve unsubscribe conflicts in mercury and mailchimp
	
	Parameters
	----------
	conflicts : Set
		list of emails to update
	status : Dict
		status data to be used in updating contacts/members
	mercury_data : Dict
		mercury data to be used in updating contacts/members
	mailchimp_data : Dict
		mailchimp data to be used in updating contacts/members
	"""

	for email in conflicts:
		mercury_unsub = mercury_data[email][0]['unsubscribed']
		mailchimp_unsub = mailchimp_data[email]['unsubscribed']
		if mailchimp_data[email]['cleaned']:
			mailchimp_unsub = mercury_unsub

		if status[email] == mercury_unsub and mercury_unsub != mailchimp_unsub:
			# case 1 => change unsubscribed in mercury
			try:
				for contact in mercury_data[email]:
					mercury_client.contacts.update(id=contact['unique_id'], data={"doNotMail":mailchimp_unsub})
				change_db('update',email,mailchimp_unsub)
				print({'email':email,'message':'was updated in mercury successfully'})
			except:
				print({'email':email,'message':'was NOT updated in mercury'})
		elif status[email] == mailchimp_unsub and mercury_unsub != mailchimp_unsub:
			# case 2 => change unsubscribed in mailchimp
			try:
				mailchimp_client.lists.members.update(
					list_id=creds.mailchimp['list_id'], 
					subscriber_hash=mailchimp_data[email]['unique_id'], 
					data={'status': mailchimp_status_convert(mercury_unsub)}
					)
				change_db('update',email,mercury_unsub)
				print({'email':email,'message':'was updated in mailchimp successfully'})
			except:
				print({'email':email,'message':'was NOT updated in mailchimp'})
		else:
			# case 3 => change unsubscribed in status
			try:
				change_db('update',email,mailchimp_unsub)
				print({'email':email,'message':'was updated in status successfully'})
			except:
				print({'email':email,'message':'was NOT updated in status'})

def remove_status_email(email_set):
	for email in email_set:
		try:
			change_db('delete',email)
			print({'email':email,'message':'was deleted from status successfully'})
		except:
			print({'email':email,'message':'was NOT deleted from status'})

def add_status_email(email_set,mercury_data,mailchimp_data,mercury_client,mailchimp_client):
	for email in email_set:
		mercury_unsub = mercury_data[email][0]['unsubscribed']
		mailchimp_unsub = mailchimp_data[email]['unsubscribed']
		if mailchimp_data[email]['cleaned']:
			mailchimp_unsub = mercury_unsub

		if mercury_unsub != mailchimp_unsub:
			# update both to True
			try:
				mailchimp_client.lists.members.update(
					list_id=creds.mailchimp['list_id'], 
					subscriber_hash=mailchimp_data[email]['unique_id'], 
					data={'status': mailchimp_status_convert(True)}
					)
				for contact in mercury_data[email]:
					mercury_client.contacts.update(id=contact['unique_id'], data={"doNotMail":True})
				print({'email':email,'message':'a conflict was resolved by setting both to True'})
			except:
				print({'email':email,'message':'a conflict could not be resolved'})
		try:
			change_db('insert',email,mercury_unsub or mailchimp_unsub)
			print({'email':email,'message':'was added to status successfully'})
		except:
			print({'email':email,'message':'was NOT added to status'})


def change_db(action,email,unsubscribed=None):
	""" make changes to status database
	
	Parameters
	----------
	action : String
		describe which sql statement to use (insert, update or delete)
	email : String
		the email (primary key) of the row to be changed
	unsubscribed : Boolean
		unsubscribed information of the row to be changed
	"""

	sql = ""
	if action == "insert":
		sql = """
			INSERT INTO status(email,unsubscribed) 
			VALUES ('{}', {}) 
			""".format(email,unsubscribed)
	elif action == "update":
		sql = """
			UPDATE status
			SET unsubscribed = {}
			WHERE status.email = '{}'
			""".format(unsubscribed,email)
	elif action == "delete":
		sql = """
			DELETE FROM status
			WHERE status.email = '{}'
			""".format(email)

	connect = None
	try:
		connect = psycopg2.connect(host=creds.db['host'],database=creds.db['database'], user=creds.db['user'], password=creds.db['password'])
		cursor = connect.cursor()
		cursor.execute(sql)
		cursor.close()
		connect.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if connect is not None:
			connect.close()

def get_status_from_db():
	""" get all data from the status database
	
	Returns 
	----------
	list
		returns a list of tuples containing the email and unsubscribed status
	"""

	connect = None
	try:
		connect = psycopg2.connect(host=creds.db['host'],database=creds.db['database'], user=creds.db['user'], password=creds.db['password'])
		cursor = connect.cursor()
		cursor.execute("""
			SELECT *
			FROM status
			""")
		result = cursor.fetchall()
		cursor.close()
		connect.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	finally:
		if connect is not None:
			connect.close()
			return result

def sync():
	""" 
	Main sync function
	"""
	print("Sync is running, time: {}".format(datetime.now()))

	# initialise clients
	mailchimp_client = MailChimp(creds.mailchimp['key'],creds.mailchimp['username'])
	mercury_client = Mercury(creds.mercury['token'],creds.mercury['key'])
	
	# get data
	mailchimp_data = get_mailchimp_data(mailchimp_client,creds.mailchimp['list_id'])
	mercury_data = get_mercury_data(mercury_client)

	# get the status of last sync
	status = {email:unsubscribed for email,unsubscribed in get_status_from_db()}

	# fix conflicts in mercury
	mercury_data = mercury_presync(mercury_data,status,mercury_client)

	# create sets
	mailchimp_set = set(mailchimp_data.keys())
	mercury_set = set(mercury_data.keys())
	status_set = set(status.keys())

	# used for conflict resolution
	intersection_set = mailchimp_set.intersection(mercury_set, status_set)

	# case 1 ==>  	new emails from mailchimp, create in mercury
	temp_set = mailchimp_set-mercury_set-status_set
	status_set = status_set.union(temp_set)
	mercury_set = mercury_set.union(temp_set)
	create_emails(temp_set, mercury_client, mailchimp_data, True)

	# case 2 ==>  	new emails from mercury, create in mailchimp
	temp_set = mercury_set-mailchimp_set-status_set
	status_set = status_set.union(temp_set)
	mailchimp_set = mailchimp_set.union(temp_set)
	create_emails(temp_set, mailchimp_client, mercury_data, False)

	# case 3 ==>	deleted emails from mercury, delete in mailchimp
	temp_set = mailchimp_set.intersection(status_set)-mercury_set
	status_set = status_set - temp_set
	mailchimp_set = mailchimp_set - temp_set
	delete_emails(temp_set, mailchimp_client, mailchimp_data)

	# case 4 ==>	Data is deleted from mailchimp, set doNotMail to True in mercury
	temp_set = mercury_set.intersection(status_set)-mailchimp_set
	status_set = status_set - temp_set
	mercury_set = mercury_set - temp_set
	unsubscribe_mercury_emails(temp_set,status,mercury_data,mercury_client)

	# case 5 ==>	Emails in status do not appear in mercury or mailchimp, remove from status
	temp_set = status_set-mercury_set.union(mailchimp_set)
	remove_status_email(temp_set)

	# case 6 ==>	Emails not in status but appear in mercury and mailchimp, add to status, what about conflicts?
	temp_set = mercury_set.union(mailchimp_set)-status_set
	add_status_email(temp_set, mercury_data, mailchimp_data, mercury_client, mailchimp_client)

	# deal with subscribed conflicts
	conflicts = [email for email in intersection_set if is_conflict([{'unsubscribed':status[email]},mercury_data[email][0],mailchimp_data[email]])]
	resolve_conflicts(conflicts,status,mercury_data,mailchimp_data,mercury_client,mailchimp_client)

	print("Sync complete")

if __name__ == "__main__":
	sync()

