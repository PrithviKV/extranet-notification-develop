require 'rubygems'
require 'pg'
require 'aws/ses'

@ses = AWS::SES::Base.new(
  :access_key_id     => 'AKIAIMWOFVXDNNECOAHA', 
  :secret_access_key => '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'
)

def send_notification_email(email_recepients, subject_line, internal_text_body, options={})

	begin 
	  o_aws_core_response = @ses.send_email(
	               :to        => email_recepients,
	               :source    => '"zumata admin" <admin@zumata.com>',
	               :subject   => "#{subject_line}",
	               :text_body => "#{internal_text_body}"
	  )
    
    email_sent_timestamp = Time.now
    
	  # o_aws_core_response_error=o_aws_core_response.try(:error)
		# if o_aws_core_response_error != nil 
		#   puts "Sending Email Notification failed, AWS:SES error #{o_aws_core_response_error.message} "
		#   return nil
		# end
	  
    return email_sent_timestamp

	  # rescue AWS::SES::Base::Errors => e
	rescue 
    # puts "AWS::SES send_email Exception: #{e.message}, #{e.backtrace}"
    return nil
	end
end

def check_for_successful_booking 
	
	subject_line = "ZUMATA: Successful Booking"
  res = @conn.exec("SELECT * FROM BOOKINGS WHERE STATUS IN ('CF') AND BOOKING_NOTIFICATION_SENT IS NULL")

  res.each do |row|
    email_recepients = []
    users = @conn.exec("SELECT USER_ID FROM MANAGED_HOTELS WHERE HOTEL_ID = #{row["hotel_id"]}")
    users.each do |u|
      email_recepient =  @conn.exec("SELECT EMAIL FROM USERS WHERE ID = #{u["user_id"]}") 
      email_recepients << email_recepient[0]["email"]
    end

    if !email_recepients.blank?

	    body = "Dear Sir/Madam,\n\n This is to notify that there was a successful booking made. Details are as below: \n\n Booking Time: #{row["created_at"]}\n Room Description: #{row["room_description"]}\n Package Label: #{row["package_label"]} \n Room Count: #{row["room_count"]}\n Adult Count: #{row["adult_count"]}\n Arrival Date: #{row["arrival_date"]}\n Departure_Date: #{row["departure_date"]}\n\n Thank you,\n Zumata Admin"

	    email_sent_timestamp = send_notification_email(email_recepients,subject_line, body)

	    if !email_sent_timestamp.blank?

        #update database with email_sent_timestamp
        @conn.exec("UPDATE BOOKINGS SET BOOKING_NOTIFICATION_SENT = '#{email_sent_timestamp}' WHERE ID = #{row["id"]}")
	    end
	  end
  end

end

def check_for_cancelled_booking 

	subject_line = "ZUMATA: Booking Cancelled"
  res = @conn.exec("SELECT * FROM BOOKINGS WHERE STATUS IN ('CX') AND CANCELLATION_NOTIFICATION_SENT IS NULL")

  res.each do |row|
	  email_recepients = []
    users = @conn.exec("SELECT USER_ID FROM MANAGED_HOTELS WHERE HOTEL_ID = #{row["hotel_id"]}")
    users.each do |u|
      email_recepient =  @conn.exec("SELECT EMAIL FROM USERS WHERE ID = #{u["user_id"]}") 
      email_recepients << email_recepient[0]["email"]
    end

    email_recepients 

    if !email_recepients.blank?

	    body = "Dear Sir/Madam,\n\n This is to notify that there was a booking cancelled. Details are as below: \n\n Booking Time: #{row["created_at"]}\n Room Description: #{row["room_description"]}\n Package Label: #{row["package_label"]} \n Room Count:  #{row["room_count"]}\n Adult Count: #{row["adult_count"]}\n Arrival Date: #{row["arrival_date"]}\n Departure_Date: #{row["departure_date"]}\n\n Thank you, Zumata Admin"
	    
	    email_sent_timestamp = send_notification_email("prithvi.kv@zumata.com", subject_line, body)
	    
	    if !email_sent_timestamp.blank?

        #update database with email_sent_timestamp
        @conn.exec("UPDATE BOOKINGS SET CANCELLATION_NOTIFICATION_SENT = '#{email_sent_timestamp}' WHERE ID = #{row["id"]}")
	    end
	  end
  end

end


def send_notification_to_zumata_team(message)
	subject_line = "Notification Service Failed"

  body = "This is to notify that Notification Email Service Failed due to the error: #{message} "
  
  send_notification_email("prithvi.kv@zumata.com",subject_line, body)
end

begin
  puts "begin"

  # $con = Mysql2::Client.new(:host => "#{ENV['DB_HOST']}", :port => '3306', :username => "#{ENV['DB_UNAME']}", :password => "#{ENV['DB_PWD']}", :database => 'vcard')
  @conn = PGconn.connect("localhost", 5432, "", "", "extranet_api_core_db_development",  )
  
  check_for_successful_booking
  check_for_cancelled_booking
  
rescue PG::Error => e

  puts " Connecting to the database was not succeccful: #{e.message} #{e.error}"

	#send email notification to zumata team
  send_notification_to_zumata_team("#{e.message}")
ensure
  puts "closing...."
  @conn.close if @conn
end