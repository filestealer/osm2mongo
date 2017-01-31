require_relative 'dbhandler'
require 'mongo'
require 'pry'

module DB
    class Mongohandler < DB::Dbhandler
        attr_accessor :connection, :collection, :host, :port
        attr_accessor :data_array

        #
        # Constructor.
        # ==== Attributes
        # * +database_name+ database name
        # * +collection_name+ collection name
        # * +bulk_limit+ Array limit for bulk insert.
        # * +host+ host name (default: localhost)
        # * +port+ port number (default: 27017)    
        def initialize(database_name, collection_name, bulk_limit, host, port)
            @host = host
            @port = port
            @dbname = database_name
            @collname = collection_name
            @bulk_limit = bulk_limit
            @data_array = Array.new
        end
        
        #
        # Use Existing Connection
        # ==== Attributes
        # * +connection+ Existing connection
        #
        def use_connection(connection)
            @connection = connection
            @collection = (@connection[@dbname])
        end

        #
        # Connecting to Mongo database.
        #
        def connect
            #@connection = Mongo::Connection.new(, @port, :pool_size => 5)
            @connection = Mongo::Client.new([ "#{@host}:#{@port}" ], :database => @dbname)
            @collection = @connection[@collname]
        end

        #
        # Insert data.
        # ==== Attributes
        # * +data+ Hash or Array
        # ==== Examples
        # insert({"key" => value})
        def insert(data)
            begin
               @collection.insert_one(data)
            rescue Exception => e
                puts e
            end
        end
    
        #
        # Insert at once many data.
        # ==== Attributes
        # * +data+ Array of Hashes
        # bulk_insert([{"key" => value},{"another_key" => another_value}])
        #
        def bulk_insert(data)
            begin
               @collection.insert_many(data)
            rescue Exception => e
                puts e
            end
        end
    
        #
        # Insert data into Data Array.
        # 
        # Purpose:
        #  When limit reached DBHandler::bulk_insert will be called on data array.
        #
        # ==== Attributes
        # * +data+ Hash or Array
        #    
        def add(data)
            if (@data_array.size >= @bulk_limit)
                bulk_insert(@data_array)
                @data_array.clear
            end

            health = [
                'baby_hatch',
                'clinic',
                'dentist',
                'doctors',
                'hospital',
                'nursing_home',
                'pharmacy',
                'social_facility',
            ]

            education = [
                'college',
                'kindergarten',
                'library',
                'public_bookcase',
                'music_school',
                'music_school',
                'driving_school',
                'language_school',
                'university',
            ]

            finance = [
                'atm',
                'bank',
                'bureau_de_change',
            ]

            food_amenity = [
                'convenience',
                'mall',
                'supermarket',
            ]
            food_shop = [
                'cafe',
                'drinking_water',
                'fast_food',
                'food_court',
                'ice_cream',
                'pub',
                'restaurant',
            ]

            entertainment = [
                'arts_centre',
                'casino',
                'cinema',
                'community_centre',
                'fountain',
                'gambling',
                'nightclub',
                'planetarium',
                'social_centre',
                'stripclub',
                'studio',
                'theatre',
            ]

            services_amenity = [
                'social_facility',
            ]
            services_office = [
                'accountant',
                'advertising_agency',
                'adoption_agency',
                'architect',
                'lawyer',
                'estate_agent',
                'copyshop',
                'funeral_directors',
            ]



            if data["tags"] != nil
                 data["loc"] = {
                    type: "Point",
                    coordinates: [data["lon"].to_f, data["lat"].to_f]
                }
                if health.include? data["tags"]["amenity"]
                    data["type"] = 'health'
                    @data_array.push(data)
                end
                if education.include? data["tags"]["amenity"]
                    data["type"] = 'education'
                    @data_array.push(data)
                end
                if finance.include? data["tags"]["amenity"]
                    data["type"] = 'finance'
                    @data_array.push(data)
                end
                if food_amenity.include?(data["tags"]["amenity"]) || food_shop.include?(data["tags"]["shop"])
                    data["type"] = 'food'
                    @data_array.push(data)
                end

                if entertainment.include? data["tags"]["amenity"]
                    data["type"] = 'entertainment'
                    @data_array.push(data)
                end

                if services_amenity.include?(data["tags"]["amenity"]) || services_office.include?(data["tags"]["office"])
                    data["type"] = 'service'
                    @data_array.push(data)
                end
            end
            #@data_array.push(data)
        end

        def flush
            # Push remaining data to database
            bulk_insert(@data_array)
            # Clear Hash Array
            @data_array.clear
            # Close Database connection.
            @connection.close()
        end
    end
end