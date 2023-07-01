from functools import reduce
from flask import jsonify
import mysql.connector as mysql
import pandas as pd


db = mysql.connect(
    host="localhost",
    user="root",
    passwd="Root@1234",
    database="books"
)


class Books(object):
    def __init__(self, *args):
        (self.id, self.languages, self.mime_types, self.topics, self.authors,
         self.titles, self.page_no) = args
        self.language_df = None
        self.author_df = None
        self.mime_df = None
        self.topics_df = None
        self.books_df = None
        self.final_df = None

    def get_books_data(self):
        return self.get_data()

    def get_language_data(self):
        sql = "SELECT p.CODE AS Language_Code, t.book_id AS Book_Id " \
                       "FROM books_language p INNER JOIN(SELECT book_id, " \
                       "language_id FROM books_book_languages) AS t ON " \
                       "t.language_id = p.id"
        if self.languages:
            sql = sql + ' AND p.CODE IN (' + ', '.join(
                self.languages) + ')'
        sql = sql + ';'
        self.language_df = pd.read_sql(sql, db)

    def get_author_data(self):
        sql = "SELECT p.name AS Author, p.birth_year AS Birth_Year, " \
              "p.death_year AS Death_Year, t.book_id AS Book_Id " \
              "FROM books_author p INNER JOIN(SELECT book_id, author_id FROM " \
              "books_book_authors) AS t ON t.author_id = p.id"
        if self.authors:
            sql = sql + ' WHERE '
            for author in self.authors:
                sql = sql + "lower(p.name) LIKE '%" + author.lower() + "%' OR "
                sql = sql[0:-4]
        sql = sql + ';'
        self.author_df = pd.read_sql(sql, db)
        self.author_df = self.author_df.drop_duplicates()

    def get_topics_data(self):
        subject_sql = "SELECT p.name AS `Subject`, shelf.bookshelf " \
                      "AS Bookshelf, t.book_id AS Book_Id " \
                       "FROM books_subject p INNER JOIN(SELECT book_id, " \
                       "subject_id FROM books_book_subjects) AS t ON " \
                       "t.subject_id = p.id LEFT OUTER JOIN(SELECT s.name AS " \
                      "bookshelf, b.book_id AS book_id FROM books_bookshelf " \
                      "s INNER JOIN(SELECT book_id, bookshelf_id FROM " \
                      "books_book_bookshelves) AS b ON b.bookshelf_id = s.id)" \
                      " AS shelf ON t.book_id = shelf.book_id"
        bookshelf_sql = "SELECT sub.subject AS `Subject`, p.name " \
                        "AS Bookshelf, t.book_id AS Book_Id " \
                        "FROM books_bookshelf p INNER JOIN(SELECT book_id, " \
                        "bookshelf_id FROM books_book_bookshelves) AS t ON " \
                        "t.bookshelf_id = p.id LEFT OUTER JOIN(SELECT s.name " \
                        "AS Subject, b.book_id AS book_id FROM " \
                        "books_subject s INNER JOIN(SELECT book_id, " \
                        "subject_id FROM books_book_subjects) AS b ON " \
                        "b.subject_id = s.id) AS sub ON " \
                        "t.book_id = sub.book_id"
        if self.topics:
            subject_sql = subject_sql + ' WHERE '
            bookshelf_sql = bookshelf_sql + ' WHERE '
            for topic in self.topics:
                subject_sql = subject_sql + "lower(p.name) LIKE '%" + \
                              topic.lower() + \
                              "%' OR "
                bookshelf_sql = bookshelf_sql + "lower(p.name) LIKE '%" + \
                                topic.lower() + "%' OR "
            subject_sql = subject_sql[0:-4]
            bookshelf_sql = bookshelf_sql[0:-4]
        subject_sql = subject_sql + ';'
        bookshelf_sql = bookshelf_sql + ';'
        subject_df = pd.read_sql(subject_sql, db)
        bookshelf_df = pd.read_sql(bookshelf_sql, db)
        self.topics_df = pd.concat([subject_df, bookshelf_df])
        self.topics_df = self.topics_df.drop_duplicates()


    def get_urls(self):
        sql = "SELECT book_id AS Book_Id, mime_type AS Mime_Type, url AS URL " \
              "from books_format"
        if self.mime_types:
            sql = sql + ' WHERE '
            for mime in self.mime_types:
                sql = sql + "mime_type LIKE '%" + mime + "%' OR "
                sql = sql[0:-4]
        sql = sql + ';'
        self.mime_df = pd.read_sql(sql, db)

    def get_books(self):
        sql = "SELECT id AS Book_Id, title AS Title, media_type AS Media, " \
              "download_count AS Download_Count FROM books_book"
        if self.id:
            sql = sql + ' WHERE gutenberg_id IN (' + ', '.join(self.id) + ')'
        if self.titles:
            if self.id:
                sql = sql + ' AND '
            else:
                sql = sql + ' WHERE '
            for title in self.titles:
                sql = sql + "lower(title) LIKE '%" + title.lower() + "%' OR "
                sql = sql[0:-4]
        sql = sql + ';'
        self.books_df = pd.read_sql(sql, db)

    def get_data(self):
        # Query language Tables to get the language codes
        self.get_language_data()

        # Query Subject Tables and Bookshelf Tables to get the Subject and
        # Bookshelf Names
        self.get_topics_data()

        # Query Author Tables to get the Author Info
        self.get_author_data()

        # Query Format Table to get the URLs
        self.get_urls()

        # Query Books Table to get the Book Details
        self.get_books()
        data_frames = [self.books_df, self.author_df, self.language_df,
                       self.topics_df, self.mime_df]
        self.final_df = reduce(lambda left, right:
                               pd.merge(left, right, on=['Book_Id']),
                               data_frames)
        self.final_df.fillna('', inplace=True)
        book_id_list = set(list(self.final_df['Book_Id']))
        total_books = len(book_id_list)
        if self.page_no == 1:
            start_record = 0
            end_record = 25
        else:
            start_record = (self.page_no - 1) * 25
            end_record = start_record + 25
        if total_books == 0:
            return jsonify({'status': 'Success', 'Total No Of Books':
                total_books, 'Total No Of Pages': 0,
                            'Current Page No': self.page_no, 'Books': []})
        elif total_books > 25:
            total_pages = int(total_books/25)
            if total_books % 25 != 0:
                total_pages += 1
        else:
            total_pages = 1

        book_list_to_send = list(book_id_list)[start_record:end_record]
        self.final_df['Selected'] = self.final_df.Book_Id.isin(
            book_list_to_send).astype(int)
        self.final_df = self.final_df[self.final_df['Selected'] == 1]
        self.final_df = self.final_df.drop_duplicates()
        book_list = []
        initial_id = None
        subject_list = list()
        bookshelf_list = list()
        mime_type = dict()
        author_info = dict()
        language = None
        records = self.final_df.to_dict(orient='records')

        if len(records) > 0:
            for data in records:
                if initial_id is None:
                    initial_id = data.get('Book_Id')
                    title = data.get('Title')
                    media = data.get('Media')
                    author_info = {'Author Name': data.get('Author'),
                                   'Birth Year': int(data.get('Birth_Year')) if
                                   data.get('Birth_Year') != '' else '',
                                   'Death Year': int(data.get('Death_Year')) if
                                   data.get('Death_Year') != '' else ''}
                    language = data.get('Language_Code')
                    if data.get('Bookshelf') != '':
                        bookshelf_list.append(data.get('Bookshelf'))
                    if data.get('Subject') != '':
                        subject_list.append(data.get('Subject'))
                    mime_type[data.get('Mime_Type')] = {'Mime Type': data.get(
                        'Mime_Type'), 'Download Link': data.get('URL')}
                if initial_id == data.get('Book_Id'):
                    if data.get('Bookshelf') != '' and data.get(
                            'Bookshelf') not in bookshelf_list:
                        bookshelf_list.append(data.get('Bookshelf'))
                    if data.get('Subject') != '' and data.get(
                            'Subject') not in subject_list:
                        subject_list.append(data.get('Subject'))
                    if data.get('Mime_Type') not in list(mime_type.keys()):
                        mime_type[data.get('Mime_Type')] = {
                            'Mime Type': data.get('Mime_Type'),
                            'Download Link': data.get('URL')}
                else:
                    book_list.append(dict(Title=title, Author_Info=author_info,
                                          Genre=media, Language=language,
                                          Subjects=subject_list,
                                          Bookshelves=bookshelf_list,
                                          Download_links=mime_type))
                    bookshelf_list = list()
                    mime_type = dict()
                    subject_list = list()
                    initial_id = data.get('Book_Id')
                    title = data.get('Title')
                    media = data.get('Media')
                    author_info = {'Author Name': data.get('Author'),
                                   'Birth Year': int(data.get('Birth_Year')) if
                                   data.get('Birth_Year') != '' else '',
                                   'Death Year': int(data.get('Death_Year')) if
                                   data.get('Death_Year') != '' else ''}
                    language = data.get('Language_Code')
                    if data.get('Bookshelf') != '':
                        bookshelf_list.append(data.get('Bookshelf'))
                    if data.get('Subject') != '':
                        subject_list.append(data.get('Subject'))
                    mime_type[data.get('Mime_Type')] = {'Mime Type': data.get(
                        'Mime_Type'), 'Download Link': data.get('URL')}
            book_list.append(dict(Title=title, Author_Info=author_info,
                                  Genre=media, Language=language,
                                  Subjects=subject_list,
                                  Bookshelves=bookshelf_list,
                                  Download_links=mime_type))

            return jsonify({'status': 'Success', 'Total No Of Books':
                total_books, 'Total No Of Pages': total_pages,
                            'Current Page No': self.page_no, 'Books':
                                book_list})
        else:
            return jsonify({'status': 'Failure', 'Total No Of Books':
                total_books, 'Total No Of Pages': total_pages,
                            'Current Page No': self.page_no, 'message':
                                'Please enter a valid page number'})












