/**
 * 
 */
package com.giantelectronicbrain.hadoop.springrepo;

import org.springframework.data.jpa.repository.JpaRepository;

import com.giantelectronicbrain.hadoop.Word;

/**
 * Define Spring JPA based repository for Word type. The Spring Data framework will
 * instantiate an actual concrete implementation which extends its CrudRepository 
 * abstraction.
 * 
 * @author tharter
 *
 */
public interface JpaWordRepository extends JpaRepository<Word, String> {

}
