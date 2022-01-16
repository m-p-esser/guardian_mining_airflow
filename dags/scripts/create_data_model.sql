CREATE TABLE `article`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `web_title` VARCHAR(200) NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `pillar` VARCHAR(100) NOT NULL,
    `section` VARCHAR(100) NOT NULL,
    `authors` VARCHAR(200) NULL,
    `type` ENUM('article') NOT NULL COMMENT 'Should be only article, just for validation purposes',
    `language` VARCHAR(20) NOT NULL,
    `web_publication_date` DATETIME NULL,
    `newspaper_edition_date` DATETIME NULL,
    `first_publication_date` DATETIME NULL,
    `newspaper_page_number` INT UNSIGNED NULL,
    `production_office` VARCHAR(30) NOT NULL,
    `publication` VARCHAR(100) NOT NULL,
    `word_count` INT UNSIGNED NOT NULL,
    `char_count` INT UNSIGNED NOT NULL,
    `web_url` VARCHAR(255) NOT NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `article_text`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `web_title` VARCHAR(200) NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `headline` TEXT NULL,
    `trail_text` TEXT NULL,
    `standfirst` TEXT NULL,
    `cover_image_caption` TEXT NULL,
    `body` TEXT NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `article_setting`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `web_title` VARCHAR(200) NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
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
    `article_id` INT UNSIGNED NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `keyword_long` VARCHAR(200) NULL,
    `keyword` VARCHAR(100) NULL,
    `section` VARCHAR(100) NULL,
    `type` ENUM('keyword') NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_contributor`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `contributor_long` VARCHAR(200) NULL,
    `contributor` VARCHAR(100) NULL,
    `first_name` CHAR(100) NULL,
    `last_name` CHAR(100) NULL,
    `type` ENUM('contributor') NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_tracking`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `tracking_long` VARCHAR(200) NULL,
    `tracking` VARCHAR(100) NULL,
    `section` VARCHAR(100) NULL,
    `type` ENUM('tracking') NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_series`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `series_long` VARCHAR(200) NULL,
    `series` VARCHAR(100) NULL,
    `section` VARCHAR(100) NULL,
    `type` ENUM('series') NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_tone`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `tone_long` VARCHAR(200) NULL,
    `tone` VARCHAR(100) NULL,
    `type` ENUM('tone') NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_newspaper_book`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `newspaper_book_long` VARCHAR(200) NULL,
    `newspaper_book` VARCHAR(100) NULL,
    `section` VARCHAR(100) NULL,
    `type` ENUM('newspaper-book') NULL,
    `inserted` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE `tag_newspaper_book_section`(
    `id` INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    `article_id` INT UNSIGNED NOT NULL,
    `web_url_suffix` VARCHAR(255) NOT NULL COMMENT 'Original ID of the Article',
    `newspaper_book_section_long` VARCHAR(200) NULL,
    `newspaper_book_section` VARCHAR(100) NULL,
    `section` VARCHAR(100) NULL,
    `type` ENUM('newspaper-book-section') NULL,
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
ALTER TABLE `tag_keyword`
ADD CONSTRAINT `tag_keyword_article_id_foreign` FOREIGN KEY(`article_id`) REFERENCES `article`(`id`);