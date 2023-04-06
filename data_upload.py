import dropbox
import os
import glob


all_files = glob.glob("/home/dosenet/tmp/dosenet/*")


#access_token = 'J3pP-JFgRPQAAAAAAAAAAd9V5TMi0QB8Sl0YwOnIammQ6JNA17e1UTlYVGoI0dlb'
#dbx = dropbox.Dropbox(access_token, timeout=1100)
dbx = dropbox.Dropbox(
    app_key = '4gvw2rkpim3tblr',
    app_secret = 'cyjzkxzixsha81t',
    oauth2_refresh_token = 'J3pP-JFgRPQAAAAAAAAAAd9V5TMi0QB8Sl0YwOnIammQ6JNA17e1UTlYVGoI0dlb'
)

for file in all_files:
    file_path = file
    destination_path = "/data/"+file_path.rsplit('/', 1)[-1]
    f = open(file_path,'rb')
    #open file in binary mode to r/w using bytes (required).
    file_size = os.path.getsize(file_path)

    CHUNK_SIZE = 4 * 1024 * 1024 #4 megabytes

    try:
        if file_size <= CHUNK_SIZE:
            dbx.files_upload(f.read(), destination_path)

        else:
            upload_session_start_result = dbx.files_upload_session_start(f.read(CHUNK_SIZE)) #read 4mb of the file, start session
            cursor = dropbox.files.UploadSessionCursor(session_id=upload_session_start_result.session_id,offset=f.tell())
            # UploadSessorCursor contains the upload session ID and the offset.
            # I'm guessing they're setting the initial offset to 0mb (right?) through f.tell()
            # f.tell(): returns an integer giving the file object’s current position in the file represented as number of bytes from the beginning of the file.

            commit = dropbox.files.CommitInfo(path=destination_path)
            #Contains the path and other optional modifiers for the future upload commit.

            while f.tell() < file_size:
                if ((file_size - f.tell()) <= CHUNK_SIZE):
                    #if remaining filesize is less than 4mb
                    print(dbx.files_upload_session_finish(f.read(CHUNK_SIZE),cursor,commit))
                    print('\n\n\ncomplete')
                    #Finish the upload session and save the uploaded data to the given file path.
                else:
                    dbx.files_upload_session_append_v2(f.read(CHUNK_SIZE),cursor)
                    #append more data to an upload session
                    cursor.offset = f.tell()
                    #offset updated to new byte position in file
                    b_left = round(((file_size-f.tell())/file_size),2)*100
                    b_left = str(b_left)+"%" if b_left < 1000 else 0
                    print("\r       {} remaining....".format(b_left), end="")
                    #fun percentage remaining printing thing
    except Exception as e:
        print("ERROR: file upload failed!")
        print("")
        print(e)
        print("")
        pass

    f.close()

    dbx.sharing_create_shared_link(destination_path)

