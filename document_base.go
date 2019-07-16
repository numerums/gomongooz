package gomongooz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

//DocumentBase : This is the base type each model needs for working with the ODM. Of course you can create your own base type but make sure
// that you implement the IDocumentBase type interface!
type DocumentBase struct {
	document   IDocumentBase     `json:"-" bson:"-"`
	collection *mongo.Collection `json:"-" bson:"-"`
	connection *Connection       `json:"-" bson:"-"`

	ID        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	CreatedAt time.Time          `json:"createdAt" bson:"createdAt"`
	UpdatedAt time.Time          `json:"updatedAt" bson:"updatedAt"`
	Deleted   bool               `json:"-" bson:"deleted"`
}

type m map[string]interface{}

//SetCollection :
func (base *DocumentBase) SetCollection(collection *mongo.Collection) {
	base.collection = collection
}

//SetDocument :
func (base *DocumentBase) SetDocument(document IDocumentBase) {
	base.document = document
}

//SetConnection :
func (base *DocumentBase) SetConnection(connection *Connection) {
	base.connection = connection
}

//GetId :
func (base *DocumentBase) GetId() primitive.ObjectID {
	return base.ID
}

//SetId :
func (base *DocumentBase) SetId(id primitive.ObjectID) {
	base.ID = id
}

//GetCreatedAt :
func (base *DocumentBase) GetCreatedAt() time.Time {
	return base.CreatedAt
}

//SetCreatedAt :
func (base *DocumentBase) SetCreatedAt(createdAt time.Time) {
	base.CreatedAt = createdAt
}

//SetUpdatedAt :
func (base *DocumentBase) SetUpdatedAt(updatedAt time.Time) {
	base.UpdatedAt = updatedAt
}

//GetUpdatedAt :
func (base *DocumentBase) GetUpdatedAt() time.Time {
	return base.UpdatedAt
}

//SetDeleted :
func (base *DocumentBase) SetDeleted(deleted bool) {
	base.Deleted = deleted
}

//IsDeleted :
func (base *DocumentBase) IsDeleted() bool {
	return base.Deleted
}

//AppendError :
func (base *DocumentBase) AppendError(errorList *[]error, message string) {

	*errorList = append(*errorList, errors.New(message))
}

//Validate :
func (base *DocumentBase) Validate(Values ...interface{}) (bool, []error) {

	return base.DefaultValidate()
}

func isObjectIdHex(objectIdString string) bool {
	objectId, err := primitive.ObjectIDFromHex(objectIdString)
	return err == nil
	/*if(err != nil){
		return false
	}else{
		return true
	}*/
}

//IsValidId :
func IsValidId(id primitive.ObjectID) bool {
	return isObjectIdHex(id.Hex())
}

func IsDuplicationError(err mongo.WriteError) bool {
	return err.Code == 11000
}

//DefaultValidate :
func (base *DocumentBase) DefaultValidate() (bool, []error) {

	documentValue := reflect.ValueOf(base.document).Elem()
	fieldType := documentValue.Type()
	validationErrors := make([]error, 0, 0)

	// Iterate all struct fields
	for fieldIndex := 0; fieldIndex < documentValue.NumField(); fieldIndex++ {

		var minLen int
		var maxLen int
		var required bool
		var err error
		var fieldValue reflect.Value

		field := fieldType.Field(fieldIndex)
		fieldTag := field.Tag

		validation := strings.ToLower(fieldTag.Get("validation"))
		validationName := fieldTag.Get("json")

		minLenTag := fieldTag.Get("minLen")
		maxLenTag := fieldTag.Get("maxLen")
		requiredTag := fieldTag.Get("required")
		modelTag := fieldTag.Get("model")
		relationTag := fieldTag.Get("relation") // Reference relation, e.g. one-to-one or one-to-many

		fieldName := fieldType.Field(fieldIndex).Name
		fieldElem := documentValue.Field(fieldIndex)

		// Get element of field by checking if pointer or copy
		if fieldElem.Kind() == reflect.Ptr || fieldElem.Kind() == reflect.Interface {
			fieldValue = fieldElem.Elem()
		} else {
			fieldValue = fieldElem
		}

		if len(minLenTag) > 0 {

			minLen, err = strconv.Atoi(minLenTag)

			if err != nil {
				panic("Check your minLen tag - must be numeric")
			}
		}

		if len(maxLenTag) > 0 {

			maxLen, err = strconv.Atoi(maxLenTag)

			if err != nil {
				panic("Check your maxLen tag - must be numeric")
			}
		}

		if len(requiredTag) > 0 {

			required, err = strconv.ParseBool(requiredTag)

			if err != nil {
				panic("Check your required tag - must be boolean")
			}

		}

		splittedFieldName := strings.Split(validationName, ",")

		validationName = splittedFieldName[0]

		if validationName == "-" {
			validationName = strings.ToLower(fieldName)
		}

		if len(relationTag) > 0 && fieldValue.Kind() == reflect.Slice && relationTag != REL_1N {
			base.AppendError(&validationErrors, L("validation.field_invalid_relation1n", validationName))
		} else if fieldValue.Kind() != reflect.Slice && relationTag == REL_1N {
			base.AppendError(&validationErrors, L("validation.field_invalid_relation11", validationName))
		}

		isSet := false

		if !fieldValue.IsValid() {
			isSet = false
		} else if fieldValue.Kind() == reflect.Ptr && !fieldValue.IsNil() {

			isSet = true

		} else if fieldValue.Kind() == reflect.Slice || fieldValue.Kind() == reflect.Map {

			isSet = fieldValue.Len() > 0

		} else if fieldValue.Kind() == reflect.Interface {

			isSet = fieldValue.Interface() != nil

		} else {

			isSet = !reflect.DeepEqual(fieldValue.Interface(), reflect.Zero(reflect.TypeOf(fieldValue.Interface())).Interface())
		}

		if required && !isSet {

			base.AppendError(&validationErrors, L("validation.field_required", validationName))
		}

		if fieldValue.IsValid() {
			if stringFieldValue, ok := fieldValue.Interface().(string); ok {

				// Regex to match a regex
				regex := regexp.MustCompile(`\/((?)(?:[^\r\n\[\/\\]|\\.|\[(?:[^\r\n\]\\]|\\.)*\])+)\/((?:g(?:im?|m)?|i(?:gm?|m)?|m(?:gi?|i)?)?)`)
				isRegex := regex.MatchString(validation)

				if isSet && minLen > 0 && len(stringFieldValue) < minLen {

					base.AppendError(&validationErrors, L("validation.field_minlen", validationName, minLen))

				} else if isSet && maxLen > 0 && len(stringFieldValue) > maxLen {

					base.AppendError(&validationErrors, L("validation.field_maxlen", validationName, maxLen))
				}

				if isSet && isRegex && !validateRegexp(validation, stringFieldValue) {

					base.AppendError(&validationErrors, L("validation.field_invalid", validationName))
				}

				if isSet && validation == "email" && !validateEmail(stringFieldValue) {

					base.AppendError(&validationErrors, L("validation.field_invalid", validationName))
				}

				if len(modelTag) > 0 {

					if !isSet || !isObjectIdHex(stringFieldValue) {

						base.AppendError(&validationErrors, L("validation.field_invalid_id", validationName))
					}
				}
			} else if fieldValue.Kind() == reflect.Interface && fieldValue.Elem().Kind() == reflect.Slice {

				slice := fieldValue.Elem()

				for index := 0; index < slice.Len(); index++ {

					if objectIdString, ok := slice.Index(index).Interface().(string); ok {

						if !isObjectIdHex(objectIdString) {
							base.AppendError(&validationErrors, L("validation.field_invalid_id", validationName))
							break
						}
					}
				}
			}
		}

	}

	return len(validationErrors) == 0, validationErrors
}

//Update :
func (base *DocumentBase) Update(content interface{}) (error, map[string]interface{}) {

	if contentBytes, ok := content.([]byte); ok {

		bufferMap := make(map[string]interface{})

		err := json.Unmarshal(contentBytes, &bufferMap)

		if err != nil {
			return err, nil
		}

		typeName := strings.ToLower(reflect.TypeOf(base.document).Elem().Name())

		if mapValue, ok := bufferMap[typeName]; ok {

			if typeMap, ok := mapValue.(map[string]interface{}); ok {

				delete(typeMap, "createdAt")
				delete(typeMap, "updatedAt")
				delete(typeMap, "id")
				delete(typeMap, "deleted")
			}

			bytes, err := json.Marshal(mapValue)

			if err != nil {
				return err, nil
			}

			err = json.Unmarshal(bytes, base.document)

			if err != nil {
				return err, nil
			}

		} else {

			return errors.New("object not wrapped in typename"), nil
		}

		return nil, bufferMap

	} else if contentMap, ok := content.(map[string]interface{}); ok {

		delete(contentMap, "createdAt")
		delete(contentMap, "updatedAt")
		delete(contentMap, "id")
		delete(contentMap, "deleted")

		bytes, err := json.Marshal(contentMap)

		if err != nil {
			return err, nil
		}

		err = json.Unmarshal(bytes, base.document)

		if err != nil {
			return err, nil
		}

		return nil, nil
	}

	return nil, nil
}

//Delete : Calling this method will not remove the object from the database. Instead the deleted flag is set to true.
// So you can use bson.M{"deleted":false} in your query to filter those documents.
func (base *DocumentBase) Delete() error {

	if IsValidId(base.ID) {

		base.SetDeleted(true)

		return base.Save()
	}

	return errors.New("Invalid object id")
}

/*
Populate works exactly like func (*Query) Populate. The only difference is that you call this method
on each model which embeds the DocumentBase type. This means that you can populate single elements or sub-sub-levels.

For example:
	User := connection.Model("User")

	user := &models.User{}

	err := User.Find().Exec(user)

	if err != nil {
		fmt.Println(err)
	}

	for _, user := range users {

		if user.FirstName == "Max" { //maybe NSA needs some information about Max's messages

			err := user.Populate("Messages")

			if err != nil {
				//some error occured
				continue
			}

			if messages, ok := user.Messages.([]*models.Message); ok {

				for _, message := range messages {

					fmt.Println(message.text)
				}
			} else {
				fmt.Println("something went wrong during cast. wrong type?")
			}
		}
	}


*/

//Populate :
func (base *DocumentBase) Populate(field ...string) error {

	if base.document == nil || base.collection == nil || base.connection == nil {
		panic("You have to initialize your document with *Model.New(document IDocumentBase) before using Populate()!")
	}

	query := &Query{
		collection: base.collection,
		connection: base.connection,
		query:      bson.M{},
		multiple:   false,
		populate:   field,
	}

	return query.runPopulation(reflect.ValueOf(base.document))
}

/*
This method saves all changes for a document. Populated relations are getting converted to object ID's / array of object ID's so you dont have to handle this by yourself.
Use this function also when the document was newly created, if it is not existent the method will call insert. During the save process createdAt and updatedAt gets also automatically persisted.

For example:

	User := connection.Model("User")

	user := &models.User{}

	User.New(user) //this sets the connection/collection for this type and is strongly necessary(!) (otherwise panic)

	user.FirstName = "Max"
	user.LastName = "Mustermann"

	err := user.Save()
*/
func (base *DocumentBase) Save() error {

	if base.document == nil || base.collection == nil || base.connection == nil {
		panic("You have to initialize your document with *Model.New(document IDocumentBase) before using Save()!")
	}

	// Validate document first

	if valid, issues := base.document.Validate(); !valid {
		return &ValidationError{&QueryError{"Document could not be validated"}, issues}
	}

	/*
	 * "This behavior ensures that writes performed in the old session are necessarily observed
	 * when using the new session, as long as it was a strong or monotonic session.
	 * That said, it also means that long operations may cause other goroutines using the
	 * original session to wait." see: http://godoc.org/labix.org/v2/mongo#Session.Clone
	 */

	session := base.connection.Client.Clone()
	defer session.Close()

	collection := session.Database(base.connection.Config.DatabaseName).Collection(base.collection.Name)

	reflectStruct := reflect.ValueOf(base.document).Elem()
	fieldType := reflectStruct.Type()
	bufferRegistry := make(map[reflect.Value]reflect.Value) //used for restoring after fields got serialized - we only save ids when not embedded

	/*
	 *	Iterate over all struct fields and determine
	 *	if there are any relations specified.
	 */
	for fieldIndex := 0; fieldIndex < reflectStruct.NumField(); fieldIndex++ {

		modelTag := fieldType.Field(fieldIndex).Tag.Get("model")       //the type which should be referenced
		relationTag := fieldType.Field(fieldIndex).Tag.Get("relation") //reference relation, e.g. one-to-one or one-to-many
		autoSaveTag := fieldType.Field(fieldIndex).Tag.Get("autosave") //flag if children of relation get automatically saved

		/*
		 *	Check if custom model and relation field tag is set,
		 *  otherwise ignore.
		 */
		if len(modelTag) > 0 {

			var fieldValue reflect.Value
			var autoSave bool
			var relation string

			field := reflectStruct.Field(fieldIndex)

			// Determine relation type for default initialization
			if relationTag == REL_11 {
				relation = REL_11
			} else if relationTag == REL_1N {
				relation = REL_1N
			} else {
				relation = REL_11 //set one-to-one as default relation
			}

			// If nil and relation one-to-many -> init field with empty slice of object ids and continue loop
			if field.IsNil() {

				if relation == REL_1N {
					field.Set(reflect.ValueOf(make([]primitive.ObjectID, 0, 0)))
				}

				continue
			}

			// Determine if relation should be autosaved
			if autoSaveTag == "true" {
				autoSave = true
			} else {
				autoSave = false //set autosave default to false
			}

			// Get element of field by checking if pointer or copy
			if field.Kind() == reflect.Ptr || field.Kind() == reflect.Interface {
				fieldValue = field.Elem()
			} else {
				fieldValue = field
			}

			/*
			 *	Detect if the field is a slice, struct or string
			 *  to handle the different types of relation. Other
			 *	types are not admitted.
			 */

			// One to many
			if fieldValue.Kind() == reflect.Slice {

				if relation != REL_1N {
					panic("Relation must be '1n' when using slices!")
				}

				sliceLen := fieldValue.Len()
				idBuffer := make([]primitive.ObjectID, sliceLen, sliceLen)

				// Iterate the slice
				for index := 0; index < sliceLen; index++ {

					sliceValue := fieldValue.Index(index)

					err, objectId := base.persistRelation(sliceValue, autoSave)

					if err != nil {
						return err
					}

					idBuffer[index] = objectId
				}

				/*
				 *	Store the original value and then replace
				 *  it with the generated id list. The value gets
				 *  restored after the model was saved
				 */

				bufferRegistry[field] = fieldValue
				field.Set(reflect.ValueOf(idBuffer))

				// One to one
			} else if (fieldValue.Kind() == reflect.Ptr && fieldValue.Elem().Kind() == reflect.Struct) || fieldValue.Kind() == reflect.String {

				if relation != REL_11 {
					panic("Relation must be '11' when using struct or id!")
				}

				var idBuffer primitive.ObjectID

				err, objectId := base.persistRelation(fieldValue, autoSave)

				if err != nil {
					return err
				}

				idBuffer = objectId

				/*
				 *	Store the original value and then replace
				 *  it with the object id. The value gets
				 *  restored after the model was saved
				 */

				bufferRegistry[field] = fieldValue
				field.Set(reflect.ValueOf(idBuffer))

			} else {
				panic(fmt.Sprintf("DB: Following field kinds are supported for saving relations: slice, struct, string. You used %v", fieldValue.Kind()))
			}

		}

	}

	var err error

	now := time.Now()

	/*
	 *	Check if Object ID is already set.
	 * 	If yes -> Update object
	 * 	If no -> Create object
	 */
	if len(base.ID) == 0 {

		base.SetCreatedAt(now)
		base.SetUpdatedAt(now)

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := collection.InsertOne(ctx, base.document)

		if err != nil {
			if IsDuplicationError(err) {
				err = &DuplicateError{&QueryError{fmt.Sprintf("Duplicate key")}}
			}
		} else {
			base.SetId(res.InsertedID)
		}

	} else {

		base.SetUpdatedAt(now)
		_, errs := collection.UpsertId(base.ID, base.document)

		if errs != nil {

			if base.IsDup(errs) {
				errs = &DuplicateError{&QueryError{fmt.Sprintf("Duplicate key")}}
			} else {
				err = errs
			}
		}
	}

	/*
	 *	Restore fields which were changed
	 *	for saving progress (object deserialisation)
	 */
	for field, oldValue := range bufferRegistry {
		field.Set(oldValue)
	}

	return err
}

func (base *DocumentBase) persistRelation(value reflect.Value, autoSave bool) (primitive.ObjectID, error) {

	// Detect the type of the value which is stored within the slice
	switch typedValue := value.Interface().(type) {

	// Deserialize objects to id
	case IDocumentBase:
		{
			// Save children when flag is enabled
			if autoSave {
				err := typedValue.Save()

				if err != nil {
					return primitive.NewObjectID(), err
				}
			}

			objectID := typedValue.GetId()

			//TODO: REFACTOR THIS
			if !IsValidId(objectID) {
				panic("DB: Can not persist the relation object because the child was not saved before (invalid id).")
			}

			return objectID, nil
		}

	// Only save the id
	case primitive.ObjectID:
		{
			//TODO: REFACTOR THIS
			if !IsValidId(typedValue) {
				panic("DB: Can not persist the relation object because the child was not saved before (invalid id).")
			}

			return typedValue, nil
		}

	case string:
		{
			if !isObjectIdHex(typedValue) {
				return primitive.NewObjectID(), &InvalidIdError{&QueryError{fmt.Sprintf("Invalid id`s given")}}
			}

			return primitive.ObjectIDFromHex(typedValue)

		}

	default:
		{
			panic(fmt.Sprintf("DB: Only type 'bson.ObjectId' and 'IDocumentBase' can be stored in slices. You used %v", value.Interface()))
		}
	}
}
