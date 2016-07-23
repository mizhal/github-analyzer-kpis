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

              kpis = {
                speed_per_year: speed_per_year(data),
                speed_per_month: speed_per_month(data),
                speed_per_week: speed_per_week(data),
                last_month_projection: last_month_projection(data),
                last_week_projection: last_week_projection(data)
              }

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
          data.lazy.map{|entry| [get_month(entry.date), entry]}
            .group_by{|month, entry| month}
            .map{|month, entries| [month, entries.collect(&:count).sum]}
        end

        def get_month gh_date
          gh_date.split("-")[1]
        end

      end

    end
  end
end