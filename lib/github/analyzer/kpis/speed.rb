module Github
  module Analyzer
    module Kpis
      # Generates a speed report, with contribution speeds calculated according
      # different periods of time
      #
      # Speed is defined as contributions per day and is calculated as a mean, that is
      # total contributions in a range of dates divided by the number of days in this 
      # range of dates
      #
      # Designed to be executed synchronous, updating data periodically
      #
      # Projections are taken yearly, i. e. if a speed is X, its projection is 365 * X
      class Speed

        def initialize catalog
          @catalog = catalog
          @stop = false
          @custom_ranges = []
        end

        ## github date ranges are composed of strings of date in US format,
        ## {start: "2016-10-01", end: "2016-11-01", name: "xxxx"}
        def set_custom_ranges ranges 
          @custom_ranges = ranges
        end

        def perform 

          viprofiles = @catalog.GithubProfiles.find_by(measure_speed: true)
          ## will need pagination
          viprofiles.each do |profile|
            has_data = @catalog.GithubRawContributionCount
              .updated_since(profile.last_updated)
              .count

            if has_data > 0

              data = @catalog.GithubRawContributionCount.last_year
                .order_by("date asc")

              ## al these processing pipelines will be translated to mongo
              ## eventually, if I would found convenient to delegate this
              ## to data layer. But not now.
              kpis = {
                speed_per_year: speed_per_year(data),
                speed_per_month: speed_per_month(data),
                speed_per_week: speed_per_week(data),
              }

              kpis = kpis.merge(
                current_week_speed: current_week_speed(kpis),
                last_week_speed: last_week_speed(kpis),
                current_month_speed: current_month_speed(kpis),
                last_month_speed: last_month_speed(kpis),
                current_week_projection: current_week_projection(kpis),
                current_month_projection: current_month_projection(kpis),
                last_month_projection: last_month_projection(kpis),
                last_week_projection: last_week_projection(kpis)
              )
              
              @custom_ranges.each do |range|
                data = @catalog.GithubRawContributionCount.in_range(range.start, range.end)

                speed = speed_per_range(data)
                kpis["speed_per_custom_range_#{range.name}"] = speed
                kpis["custom_range_#{range.name}_projection"] = 365 * speed
              end

              profile.speed_kpis = kpis

            end

            yield 

            break if @stop
          end

        end

        def stop
          @stop = true
        end

        private 

        def speed_per_range data
          data.reduce{|acc, entry| acc + entry.count} / data.count
        end

        def speed_per_year(data)
          speed_per_range(data)
        end

        def speed_per_month data
          ## data -(map)-> KEY: month -(reduce)-> SUM
          ## I think this could be better done inside Mongo itself
          pairs = data.lazy.map{|entry| [get_month(entry.date), entry]}
            .group_by{|month, entry| month}
            .map{|month, entries| [month, entries.collect(&:count).sum]}
          Hash[paris]
        end

        def speed_per_week data
          ## data -(map)-> KEY: week -(reduce)-> SUM
          ## I think this could be better done inside Mongo itself
          pairs = data.lazy.map{|entry| [get_week(entry.date), entry]}
            .group_by{|week, entry| entry}
            .map{|week, entries| [week, entries.collect(&:count).sum]}
          Hash[pairs]
        end

        def current_week_speed(kpis)
          cweek = DateTime.now.cweek

          kpis[:speed_per_week][cweek]
        end

        def last_week_speed(kpis)
          lweek = DateTime.now.cweek - 1
          if lweek == 0
            lweek = 53
          end

          kpis[:speed_per_week][lweek]
        end

        def current_month_speed(kpis)
          cmonth = DateTime.now.month

          kpis[:speed_per_month][cmonth]
        end
        
        def last_month_speed(kpis)
          lmonth = DateTime.now.month - 1
          if lmonth == 0
            lmonth = 12
          end

          kpis[:speed_per_month][lmonth]
        end

        def current_week_projection(kpis)
          s = current_week_speed(kpis)
          s * 365
        end

        def current_month_projection(kpis)
          s = current_month_speed(kpis)
          s * 365
        end

        def last_month_projection(kpis)
          s = last_month_speed(kpis)
          s * 365
        end

        def last_week_projection(kpis)
          s = last_week_speed(kpis)
          s * 365
        end

        def get_month gh_date
          gh_date.split("-")[1].to_i
        end

        def get_week gh_date
          ## "2016-1-1" -> split "-" -> ["2016", "1", "1"] -> map to_i ->  
          ## [2016, 1, 1], then "explode"
          ## Maybe Mongo could do this also
          dt = DateTime.new *gh_date.split("-").collect(:to_i)
          return dt.cweek
        end

      end

    end
  end
end