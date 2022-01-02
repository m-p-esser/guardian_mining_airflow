CREATE TABLE `article`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `web_title` VARCHAR(100) NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `pillar` VARCHAR(50) NOT NULL,
    `section` VARCHAR(50) NOT NULL,
    `authors` VARCHAR(100) NULL,
    `type` ENUM('article') NOT NULL COMMENT 'Should be only article, just for validation purposes',
    `language` VARCHAR(20) NOT NULL,
    `web_publication_date` DATETIME NULL,
    `newspaper_edition_date` DATETIME NULL,
    `first_publication_in` ENUM('Web', 'Print') NOT NULL COMMENT 'Web or Print',
    `published_in_newspaper` BOOLEAN NOT NULL,
    `newspaper_page_number` INT UNSIGNED NULL,
    `production_office` VARCHAR(10) NOT NULL,
    `publication` VARCHAR(50) NOT NULL,
    `word_count` INT UNSIGNED NOT NULL,
    `char_count` INT UNSIGNED NOT NULL,
    `web_url` VARCHAR(255) NOT NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `article_text`(
    `id` INT UNSIGNED NOT NULL PRIMARY AUTO_INCREMENT,
    `web_title` VARCHAR(100) NOT NULL,
    `headline` VARCHAR(100) NULL,
    `trail_text` VARCHAR(255) NULL,
    `standfirst` VARCHAR(255) NULL,
    `cover_image_caption` VARCHAR(150) NULL,
    `body` TEXT NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `article_setting`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `is_premoderated` BOOLEAN NULL,
    `commentable` BOOLEAN NULL COMMENT 'Comments will be checked by a moderator prior to publication if true',
    `comment_close_date` DATETIME NULL,
    `should_hide_adverts` BOOLEAN NULL COMMENT 'Adverts will not be displayed if true',
    `is_hosted` BOOLEAN NULL,
    `show_in_related_content` BOOLEAN NULL COMMENT 'Whether this content can appear in automatically generated Related Content',
    `show_affiliate_links` BOOLEAN NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_keyword`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED UNIQUE NOT NULL,
    `keyword_long` VARCHAR(100) NULL,
    `keyword` VARCHAR(50) NULL,
    `section` VARCHAR(50) NULL,
    `type` ENUM('keyword') NULL,
    `web_url` VARCHAR(150) NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_contributor`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED UNIQUE NOT NULL,
    `contributor_long` VARCHAR(100) NULL,
    `contributor` VARCHAR(50) NULL,
    `first_name` CHAR(50) NULL,
    `last_name` CHAR(50) NULL,
    `type` ENUM('contributor') NULL,
    `web_url` VARCHAR(150) NULL,
    `inserted` DATETIME NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_tracking`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED UNIQUE NOT NULL,
    `tracking_long` VARCHAR(100) NULL,
    `tracking` VARCHAR(50) NULL,
    `type` ENUM('tracking') NULL,
    `web_url` VARCHAR(150) NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_series`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED UNIQUE NOT NULL,
    `series_long` VARCHAR(100) NULL,
    `series` VARCHAR(50) NULL,
    `section` VARCHAR(50) NULL,
    `type` ENUM('series') NULL,
    `web_url` VARCHAR(150) NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_tone`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED UNIQUE NOT NULL,
    `tone_long` VARCHAR(100) NULL,
    `tone` VARCHAR(50) NULL,
    `type` ENUM('tone') NULL,
    `web_url` VARCHAR(150) NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_newspaper_book`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED UNIQUE NOT NULL,
    `newspaper_book_long` VARCHAR(100) NULL,
    `newspaper_book` VARCHAR(50) NULL,
    `section` VARCHAR(50) NULL,
    `type` ENUM('newspaper-book') NULL,
    `web_url` VARCHAR(150) NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_newspaper_book_section`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED UNIQUE NOT NULL,
    `newspaper_book_section_long` VARCHAR(100) NULL,
    `newspaper_book_section` VARCHAR(50) NULL,
    `section` VARCHAR(255) NULL,
    `type` ENUM('newspaper-book-section') NULL,
    `web_url` VARCHAR(150) NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE `tag_series`
ADD CONSTRAINT `tag_series_article_id_foreign` FOREIGN KEY(`article_id`) REFERENCES `article`(`id`);
ALTER TABLE `tag_tone`
ADD CONSTRAINT `tag_tone_article_id_foreign` FOREIGN KEY(`article_id`) REFERENCES `article`(`id`);
ALTER TABLE `tag_tracking`
ADD CONSTRAINT `tag_tracking_article_id_foreign` FOREIGN KEY(`article_id`) REFERENCES `article`(`id`);
ALTER TABLE `tag_newspaper_book_section`
ADD CONSTRAINT `tag_newspaper_book_section_article_id_foreign` FOREIGN KEY(`article_id`) REFERENCES `article`(`id`);
ALTER TABLE `tag_newspaper_book`
ADD CONSTRAINT `tag_newspaper_book_article_id_foreign` FOREIGN KEY(`article_id`) REFERENCES `article`(`id`);
ALTER TABLE `tag_contributor`
ADD CONSTRAINT `tag_contributor_article_id_foreign` FOREIGN KEY(`article_id`) REFERENCES `article`(`id`);